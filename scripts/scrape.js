/*
  Scrape to do
    1. implement more scrapers
*/

'use strict'

const fs                = require('fs')
const _                 = require('lodash')
const Bluebird          = require('bluebird')
const request           = require('request')
const js_utils          = require('../utils/js_utils')
const pg                = require('../utils/js_postgres')
const argv              = require('minimist')(process.argv.slice(2))

const config_public     = require('../incl/config_public')
const config_secret     = require('../incl/config_secret')
const config = {
  ...config_public,
  ...config_secret
}

const ROW_CONCURRENCY = argv.concurrency || 8
const VERBOSE         = argv.verbose
const REQUIRED_ARGS   = []

const TIMEOUT                       = 10000
const MAX_API_HITS                  = 50

const ENDPOINTS = {
  auto_trader : 'https://www.autotrader.com/rest/searchresults/base',
  autolist    : 'https://www.autolist.com/search',
  'cars.com'  : 'https://www.cars.com/for-sale/listings/',
  edmunds     : 'https://www.edmunds.com/gateway/api/purchasefunnel/v1/srp/inventory',
}

const AUTO_TRADER_FIELDS_TO_KEEP = [
  'year',
  'price',
  'vin',
  'title',
  'zip'
]

const CURL_USER_AGENT = { 'User-Agent': 'curl/7.64.1' }
const DEFAULT_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36'
}

const PG_DUPE_ERROR   = "duplicate key value violates unique constraint"

const INSERT_VEHICLE_QUERY = `
  insert into vehicles (
    vin,
    make, model, version, year
  ) values (
    :vin,
    :make, :model, :version, :year
  )
`

const INSERT_LISTING_QUERY = `
  insert into vehicle_listings (
    created_on,
    vin,
    source,
    owner, zip, remote,
    mileage, price,
    title
  ) values (
    :scrape_time,
    :vin,
    :source,
    :owner, :zip, :remote,
    :mileage, :price,
    :title
  )
`

const get_model_configs = function(scrape_configs, location_config, model, source) {
  if (!scrape_configs[model].scrape_params[source]) throw Error(`scrape_parms not found: ${model} / ${source}`)
  return {
    model_config  : scrape_configs[model],
    scrape_config : scrape_configs[model].scrape_params[source]
  }
} 


/* 
  easiest: found endpoints return json 
*/
const scrape_auto_trader = async function(scrape_configs, location_config, model) {
  const source = 'auto_trader'
  const {model_config, scrape_config} = get_model_configs(scrape_configs, location_config, model, source)

  const headers = {
    ...DEFAULT_HEADERS,
    ...CURL_USER_AGENT    // if using regular user-agent, sends back 200 & with html of error page
  }
  const qs = {
    ..._.pick(scrape_config, ['makeCodeList', 'modelCodeList']),
    startYear       : model_config.min_year,
    maxPrice        : model_config.max_price,
    maxMileage      : model_config.max_miles,
    city            : location_config.city,
    state           : location_config.state,
    zip             : location_config.zip,
    searchRadius    : model_config.radius,
    sortBy          : 'derivedpriceASC',
    numRecords      : 100,

    // extra request params
    allListingType  : 'all-cars',
    channel         : 'ATC',
    isNewSearch     : true
  }
  const options = {
    url     : ENDPOINTS[source],
    method  : 'GET',
    timeout : TIMEOUT,
    headers,
    qs
  }

  let scrape_results = []
  try {
    scrape_results = (await js_utils.make_request(options)).listings
  } catch (scrape_error) {
    const scrape_error_str = JSON.stringify(scrape_error_str) !== '[object Object]' ? JSON.stringify(scrape_error_str) : String(scrape_error_str)
    console.error(`error scraping, skipping source: ${JSON.stringify({ source, scrape_error_str })}`)
  }

  return _.map(scrape_results, listing =>  {
    return {
      ..._.pick(listing, ['vin', 'year', 'zip', 'title']),
      version       : listing.trim,
      price         : parseInt(listing.pricingDetail.derived.replace(',', '').substring(1)),
      mileage       : listing.specifications.mileage.value ? parseInt(listing.specifications.mileage.value.replace(',', '')) : null,
      owner         : listing.ownerName,
      remote        : false,
      scrape_time   : (new Date()).toString(),
      source
    }
  })
}

// note: search is nationwide including cars that can be transfered locally
  // ordering by prices asc would include lots of remote options & skew results towards cheaper listings
  // ordering by distance asc would omit cheap national cars
  // for now, ordering by auotlist's best match...
const scrape_autolist = async function(scrape_configs, location_config, model) {
  const source = 'autolist'
  const {model_config, scrape_config} = get_model_configs(scrape_configs, location_config, model, source)

  const headers = DEFAULT_HEADERS
  const qs = {
    ..._.pick(scrape_config, ['make', 'model']),
    year_min        : model_config.min_year,
    price_max       : model_config.max_price,
    mileage         : model_config.max_miles,
    latitude        : location_config.lat,
    longitude       : location_config.lon,
    radius          : model_config.radius,
    sort_filter     : /*'price:asc',*/ '' // see note about ordering
  }
  const options = {
    url     : ENDPOINTS[source],
    method  : 'GET',
    timeout : TIMEOUT,
    headers,
    qs
  }
  
  let scrape_results = []
  let continue_scrape = true, page = 1, api_hits = 0
  try {
    while (continue_scrape) {
      if (api_hits >= MAX_API_HITS) throw Error(`max api hits exceeded, stopping scrape: ${JSON.stringify({ api_hits, MAX_API_HITS, current_results_count: scrape_results.length })}`)

      options.qs.page = page
      const response = await js_utils.make_request(options)
      scrape_results = scrape_results.concat(response.records)

      page++
      api_hits++
      continue_scrape = response.total_count > scrape_results.length
    }
  } catch (scrape_error) {
    const scrape_error_str = JSON.stringify(scrape_error) !== '{}' ? JSON.stringify(scrape_error) : String(scrape_error)
    console.error(`error scraping, continuing with current results: ${JSON.stringify({ source, scrape_error_str, current_results_count: scrape_results.length })}`)
  }

  return _.map(scrape_results, listing => {
    return {
      ..._.pick(listing, ['vin', 'year']),
      version       : listing.trim,
      price         : parseInt(listing.price.replace(',', '').substring(1)),
      mileage       : parseInt(listing.mileage.replace(',', '').replace('Miles', '')),
      owner         : listing.dealer_name,
      scrape_time   : (new Date()).toString(),
      source,
      remote        : listing.distance_from_origin > model_config.radius
    }
  })
}


/*
  medium: partial server-side rendering
*/

// notes
  // cannot replicate endpoint's year filter, must do after gathering results
const scrape_cars_dot_com = async function(scrape_configs, location_config, model) {
  const source = 'cars.com'
  const {model_config, scrape_config} = get_model_configs(scrape_configs, location_config, model, source)

  const headers = {
    ...DEFAULT_HEADERS,
    ...CURL_USER_AGENT    // request times out with default user-agent
  }
  const qs = {
    ..._.pick(scrape_config, ['mkId', 'mdId', 'mlgId']),
    prMx            : model_config.max_price,
    zc              : location_config.zip,
    rd              : model_config.radius,
    sort            : 'price-lowest',
    perPage         : 100,
    returnRecs      : false,
    searchSource    : 'PAGINATION'
  }
  const options = {
    url     : ENDPOINTS[source],
    method  : 'GET',
    timeout : TIMEOUT,
    headers,
    qs
  }
  
  let scrape_results = []
  let continue_scrape = true, page = 1, api_hits = 0
  try {
    while (continue_scrape) {
      if (api_hits >= MAX_API_HITS) throw Error(`max api hits exceeded, stopping scrape: ${JSON.stringify({ api_hits, MAX_API_HITS, page })}`)

      options.qs.page = page
      const response = await js_utils.make_request(options)
      scrape_results = scrape_results.concat(response.dtm.vehicle)

      continue_scrape = response.json.pagination.numberOfPages > page
      page++
      api_hits++
    }
  } catch (scrape_error) {
    const scrape_error_str = JSON.stringify(scrape_error) !== '{}' ? JSON.stringify(scrape_error) : String(scrape_error)
    console.error(`error scraping, continuing with current results: ${JSON.stringify({ source, scrape_error_str, current_results_count: scrape_results.length })}`)
  }

  return _.map(_.filter(scrape_results, listing => listing.year >= model_config.min_year), listing => {
    return {
      ..._.pick(listing, ['vin', 'year', 'price', 'mileage']),
      version       : listing.trim,
      owner         : `cars.com_${listing.customerId}`,
      scrape_time   : (new Date()).toString(),
      source,
      remote        : false
    }
  })
}

// NOT WORKING ON LAMBDA
  // getting ESOCKETTIMEDOUT, in dev got this with request that didn't exactly match browser
  // since works locally, is probably IP checking <- local had actual browser activity too
    // possibly solvable with one-time headless browser activity each run
  // update: implemented one-time puppeteer page loads to simulate normal activity from lambda IP
    // BUT puppeteer's chromium package too big for lambda (max 250 MB) <- possible workaround is to use docker
const scrape_edmunds = async function(scrape_configs, location_config, model) {
  const source = 'edmunds'
  const {model_config, scrape_config} = get_model_configs(scrape_configs, location_config, model, source)

  const headers = {
    ...DEFAULT_HEADERS,
    'accept'          : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-encoding' : 'gzip, deflate, br',
    'accept-language' : 'en-US,en;q=0.9',
    'connection'      : 'keep-alive',
    'sec-fetch-dest'  : 'document',
    'sec-fetch-mode'  : 'navigate',
    'sec-fetch-site'  : 'none',
    'upgrade-insecure-requests': 1
  }
  const qs = {
    ..._.pick(scrape_config, ['make', 'model']),
    ..._.pick(location_config, ['lat', 'lon', 'zip']),
    displayPrice    : `10000-${model_config.max_price}`,
    dma             : 807,
    inventoryType   : 'used,cpo',
    mileage         : `0-${model_config.max_miles}`,
    pageNum         : null,
    radius          : model_config.radius,
    sortBy          : 'price:asc',
    year            : `${model_config.min_year}-*`,
    challenger      : 'blt-1094-boost6:chal-2-i',
    fetchSuggestedFacets : true
  }
  const options = {
    url     : ENDPOINTS[source],
    method  : 'GET',
    timeout : TIMEOUT,
    headers,
    qs,
    gzip    : true
  }

  let scrape_results = []
  let continue_scrape = true, page = 1, api_hits = 0
  try {
    while (continue_scrape) {
      if (api_hits >= MAX_API_HITS) throw Error(`max api hits exceeded, stopping scrape: ${JSON.stringify({ api_hits, MAX_API_HITS, page })}`)

      options.qs.pageNum = page
      const response = await js_utils.make_request(options)
      scrape_results = scrape_results.concat(response.inventories.results)

      continue_scrape = response.inventories.totalPages > page
      page++
      api_hits++
    }
  } catch (scrape_error) {
    const scrape_error_str = JSON.stringify(scrape_error) !== '{}' ? JSON.stringify(scrape_error) : String(scrape_error)
    console.error(`error scraping, continuing with current results: ${JSON.stringify({ source, scrape_error_str, current_results_count: scrape_results.length })}`)
  }

  return _.map(scrape_results, listing => {
    return {
      ..._.pick(listing, ['vin']),
      year          : listing.vehicleInfo.styleInfo.year,
      price         : listing.prices.displayPrice,
      mileage       : listing.vehicleInfo.mileage,
      version       : listing.vehicleInfo.styleInfo.trim,
      owner         : listing.dealerInfo.name,
      zip           : parseInt(listing.dealerInfo.address.zip),
      scrape_time   : (new Date()).toString(),
      source,
      remote        : listing.dealerInfo.distance > model_config.radius
    }
  })
}

// good candiate
const scrape_car_gurus = async function(scrape_configs, location_config, model) { }

// good candiate
const scrape_truecar = async function(scrape_configs, location_config, model) { }

// good candidate
const scrape_carvana = async function(scrape_configs, location_config, model) { }

// need to filter post-request: mileage, year, price
const scrape_carfax = async function(scrape_configs, location_config, model) { }

/*
  hard: few search paramaters, uses cookies
*/
const scrape_carmax = async function(scrape_configs, location_config, model) {
  // yet unimplemented
    // carmax apis also return html...
      // https://www.carmax.com/cars/jeep/grand-cherokee
        // also can only enter search params in UI, recorded by cookies...
    // with JS disabled, site is unusable... can't apply any search filters
}

const setup_scrape = async function() {
  const HEADLESS_URLS = [
    // 'http://www.edmunds.com/',
    'https://www.edmunds.com/used-jeep-grand-cherokee/',
    'https://www.edmunds.com/used-jeep-compass/',
  ]
  const HEADLESS_DENIALS = [
    '<TITLE>Access Denied</TITLE>'
  ]
  await Bluebird.map(HEADLESS_URLS, async url => {
    await js_utils.make_headless_request(url, {
      verbose       : true,
      user_agent    : DEFAULT_HEADERS['User-Agent'],
      wait_until    : 'domcontentloaded',
      detect_denied : HEADLESS_DENIALS,
      timeout       : 5000,
      stay_on_page  : 5000,
    })
  })
}

const scrape = async function(scrape_configs, location_config, model) {
  // await setup_scrape() // removed puppeteer, was too big for lambda
  let results = {}

  results.auto_trader = await scrape_auto_trader(scrape_configs, location_config, model)
  results.autolist    = await scrape_autolist(scrape_configs, location_config, model)
  results.cars_dot_com    = await scrape_cars_dot_com(scrape_configs, location_config, model)
  // results.edmunds    = await scrape_edmunds(scrape_configs, location_config, model) // lambda IP gets denied... tried to use headless browser to simuluate normal traffic but chromium is too big for lambda

  const total = _.reduce(results, (sum, source_results, source) => sum + source_results.length, 0)
  const source_counts = _.chain(results)
    .map((source_results, source) => [source, source_results.length])
    .fromPairs()
    .value()

  const scrape_diagnostics = { total, source_counts }
  console.log(`${model}: finished scraping: ${JSON.stringify({ scrape_diagnostics })}`)

  return {
    results : _.flatten(Object.values(results)),
    scrape_diagnostics
  }
}

const insert_results = async function(results, model) {
  const insert_diagnostics = {
    insert_counts : {},
    dupe_counts   : {}
  }
  await Bluebird.map(results, async row => {
    
    // try to insert vehicle
    try {
      await pg.query(INSERT_VEHICLE_QUERY, {
        make    : config.naming_configs[model].make,
        model   : config.naming_configs[model].model,
        ..._.pick(row, ['vin', 'year', 'version'])
      })
      if (!insert_diagnostics.insert_counts.vehicles) insert_diagnostics.insert_counts.vehicles = 0
      insert_diagnostics.insert_counts.vehicles++
    } catch (pg_error) {
      if (String(pg_error).includes(PG_DUPE_ERROR)) {
        if (VERBOSE) console.log(`found duped vehicle, skipping: ${row.vin}`)
        if (!insert_diagnostics.dupe_counts.vehicles) insert_diagnostics.dupe_counts.vehicles = 0
        insert_diagnostics.dupe_counts.vehicles++
      }
      else throw pg_error
    }

    // insert listing
    const date_string = new Date(row.scrape_time).toUTCString()
    try {
      await pg.query(INSERT_LISTING_QUERY, {
        scrape_time : date_string,
        mileage     : row.mileage   || 0,
        price       : row.price     || 0,
        ..._.pick(row, ['vin', 'owner', 'zip', 'remote', 'title', 'source'])
      })
      if (!insert_diagnostics.insert_counts.listings) insert_diagnostics.insert_counts.listings = 0
      insert_diagnostics.insert_counts.listings++
    } catch (pg_error) {
      if (String(pg_error).includes(PG_DUPE_ERROR)) {
        if (VERBOSE) console.log(`found duped listing, skipping: ${JSON.stringify(_.pick(row, ['vin', 'scrape_time']))}`)
        if (!insert_diagnostics.dupe_counts.listings) insert_diagnostics.dupe_counts.listings = 0
        insert_diagnostics.dupe_counts.listings++
      }
      else throw pg_error
    }
  }, {concurrency: ROW_CONCURRENCY})
  
  console.log(`${model}: finished inserting scrape results: ${JSON.stringify({ insert_diagnostics })}\n`)
  return { insert_diagnostics }
}

pg.connect(config.pg_config)


const scrape_and_log = async () => {
  js_utils.require_args(argv, REQUIRED_ARGS)

  const diagnostics = {}
  await Bluebird.each(Object.keys(config.scrape_configs), async model => {
    const { results, scrape_diagnostics } = await scrape(config.scrape_configs, config.location_config, model)  
    const { insert_diagnostics } = await insert_results(results, model)

    diagnostics[model] = {}
    diagnostics[model].scrape_diagnostics = scrape_diagnostics
    diagnostics[model].insert_diagnostics = insert_diagnostics
  })

  return diagnostics
}

module.exports = { scrape_and_log }

if (require.main === module) {
  scrape_and_log()
    .catch((err) => {
      console.error(err)
      process.exit(1)
    })
    .then(() => process.exit(0))
}
