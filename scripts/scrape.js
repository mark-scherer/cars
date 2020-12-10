/*
  Scrape to do
    1. investigate owner DB issue
    2. investigate autolist 403's
    3. implement more scrapers
*/

'use strict'

const fs                = require('fs')
const _                 = require('lodash')
const Bluebird          = require('bluebird')
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

const AUTO_TRADER_BASE_PATH         = 'https://www.autotrader.com'
const AUTO_TRADER_SEARCH_ENDPOINT   = 'rest/searchresults/base'
const AUTOLIST_BASE_PATH            = 'https://www.autolist.com'
const AUTOLIST_SEARCH_ENDPOINT      = 'search'

const AUTO_TRADER_FIELDS_TO_KEEP = [
  'year',
  'price',
  'vin',
  'title',
  'zip'
]

const DEFAULT_HEADERS = {
  // 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36'
  'User-Agent': 'curl/7.64.1'
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
    owner, zip,
    mileage, price,
    title
  ) values (
    :scrape_time,
    :vin,
    :source,
    :owner, :zip,
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
  Easiest: found endpoints return json 
*/
const scrape_auto_trader = async function(scrape_configs, location_config, model) {
  const source = 'auto_trader'
  const {model_config, scrape_config} = get_model_configs(scrape_configs, location_config, model, source)

  const qs = {
    allListingType  : 'all-cars',
    makeCodeList    : scrape_config.make,
    modelCodeList   : scrape_config.model,
    startYear       : model_config.min_year,
    maxPrice        : model_config.max_price,
    maxMileage      : model_config.max_miles,
    city            : location_config.city,
    state           : location_config.state,
    zip             : location_config.zip,
    searchRadius    : model_config.radius,
    sortBy          : 'derivedpriceASC',
    numRecords      : 100
  }
  const options = {
    url     : `${AUTO_TRADER_BASE_PATH}/${AUTO_TRADER_SEARCH_ENDPOINT}`,
    method  : 'GET',
    timeout : TIMEOUT,
    headers : DEFAULT_HEADERS,
    qs
  }
  const body = await js_utils.make_request(options)

  return _.map(body.listings, listing =>  {
    return {
      ..._.pick(listing, ['vin', 'year', 'zip', 'title']),
      version       : listing.trim,
      price         : parseInt(listing.pricingDetail.derived.replace(',', '').substring(1)),
      mileage       : listing.specifications.mileage.value ? parseInt(listing.specifications.mileage.value.replace(',', '')) : null,
      owner         : listing.ownerName,
      scrape_time   : (new Date()).toString(),
      source
    }
  })
}

// NOTE: keep getting 403's... maybe getting blocked?
  // UNVERIFIED
const scrape_autolist = async function(scrape_configs, location_config, model) {
  throw Error(`need to investigate 403's!`)

  const source = 'autolist'
  const {model_config, scrape_config} = get_model_configs(scrape_configs, location_config, model, source)

  const qs = {
    make            : scrape_config.make,
    model           : scrape_config.model,
    year_min        : model_config.min_year,
    price_max       : model_config.max_price,
    mileage         : model_config.max_miles,
    latitude        : location_config.lat,
    longitude       : location_config.lon,
    radius          : model_config.radius,
    sort_filter     : 'price:asc',
  }
  const options = {
    url     : `${AUTOLIST_BASE_PATH}/${AUTOLIST_SEARCH_ENDPOINT}`,
    method  : 'GET',
    timeout : TIMEOUT,
    headers : DEFAULT_HEADERS,
    qs
  }
  const body = await js_utils.make_request(options)

  return _.map(body.records, listing => {
    return {
      ..._.pick(listing, ['vin', 'year']),
      version       : listing.trim,
      price         : parseInt(listing.price.replace(',', '').substring(1)),
      mileage       : parseInt(listing.mileage.replace(',', '').replace('Miles', '')),
      owner         : listing.dealer_name,
      scrape_time   : (new Date()).toString(),
      source
    }
  })
}


/*
  medium: partial server-side rendering
*/
const scrape_cars_dot_com = async function(scrape_configs, location_config, model) {
  // yet unimplemented...
    // cars.com apis return:
      // for-sale/searchresults.action/: returns page html, will need to be parsed
      // for-sale/listings/: returns json of html, unsure how parsing will work
    // with JS disabled, site is unusable... can't apply any search filters, sort
}

const scrape_carfax = async function(scrape_configs, location_config, model) {
  // yet unimplemented...
    // carfax apis return html, would need to be parsed
      // apis don't take qs, url is hardcoded with code for params: make, model and location
      // must further filter results to mileage, year, price
    // JS disabled doesn't seem to change anything
}

const scrape_edmunds = async function(scrape_configs, location_config, model) {
  // yet unimplemented
    // edmunds apis return html:
      // inventory/srp.html: looks like json vehicle info located in $luckdragon
    // JS disabled doesn't seem to change anything
}

const scrape_car_gurus = async function(scrape_configs, location_config, model) {
  // yet unimplemented
    // car gurus apis return html:
      // Cars/inventorylisting/viewDetailsFilterViewInventoryListing.action: can parse "listings"
    // with JS diasbled, api changes to Cars/l-Used-Jeep-Grand-Cherokee-d490 <- might be with JS enabled as well
}

const scrape_truecar = async function(scrape_configs, location_config, model) {
  // yet unimplemented
    // api: used-cars-for-sale/listings/jeep/grand-cherokee/
    // parse: <script>window.__INITIAL_STATE__={...}</script>
}

const scrape_carvana = async function(scrape_configs, location_config, model) {
  // yet unimplemented
    // api: cars/jeep-grand-cherokee
    // parse: <script data-react-helmet="true" type="application/ld+json">{...}</script>
}


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


const scrape = async function(scrape_configs, location_config, model) {
  let results = {}

  results.auto_trader = await scrape_auto_trader(scrape_configs, location_config, model)
  // results.autolist    = await scrape_autolist(scrape_configs, location_config, model) <- getting consistent 403's

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
      else throw Error(`insert vehicle pg_error: ${pg_error}`)
    }

    // insert listing
    const date_string = new Date(row.scrape_time).toUTCString()
    try {
      await pg.query(INSERT_LISTING_QUERY, {
        scrape_time : date_string,
        mileage     : row.mileage   || 0,
        ..._.pick(row, ['vin', 'owner', 'zip', 'price', 'title', 'source'])
      })
      if (!insert_diagnostics.insert_counts.listings) insert_diagnostics.insert_counts.listings = 0
      insert_diagnostics.insert_counts.listings++
    } catch (pg_error) {
      if (String(pg_error).includes(PG_DUPE_ERROR)) {
        if (VERBOSE) console.log(`found duped listing, skipping: ${JSON.stringify(_.pick(row, ['vin', 'scrape_time']))}`)
        if (!insert_diagnostics.dupe_counts.listings) insert_diagnostics.dupe_counts.listings = 0
        insert_diagnostics.dupe_counts.listings++
      }
      else throw Error(`insert vehicle pg_error: ${pg_error}`)
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
