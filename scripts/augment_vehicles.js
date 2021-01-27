/*
  augment_vehicles.js
    - add additional vehicle (not listing) details post-scraping
    - includes model validation - some scraped sites return listings inconsistent with request model

  TO DO
    - add more fields to augment
      - FWD/AWD
      - color
*/

'use strict'

const fs                = require('fs')
const _                 = require('lodash')
const Bluebird          = require('bluebird')
const cheerio           = require('cheerio')
const zipcodes          = require('zipcodes')
const js_utils          = require('../utils/js_utils')
const pg                = require('../utils/js_postgres')

const config_public     = require('../incl/config_public')
const config_secret     = require('../incl/config_secret')
const { sortedLastIndex } = require('lodash')
const js_postgres = require('../utils/js_postgres')
const config = {
  ...config_public,
  ...config_secret
}
pg.connect(config.pg_config)

const TIMEOUT                   = 10000
const VEHICLE_CONCURRENCY       = 8
const MAX_VEHICLE_RATE          = 25      // vehicles per second
const PAUSED_RETRY_DELAY        = 100     // ms

const DEFAULT_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36'
}

const ENDPOINT_MAP = {
  autolist  : 'https://www.autolist.com/api/vehicles',
  edmunds   : 'https://www.edmunds.com/jeep/'
}

const API_MAP = {
  edmunds: {
    model: {
      grand_cherokee  : 'grand-cherokee',
      compass         : 'compass'
    }
  }
}

const MODEL_NAME_MAP = {
  autolist: {
    patriot           : 'patriot',
    compass           : 'compass',
    cherokee          : 'cherokee',
    renegade          : 'renegade',
    liberty           : 'liberty',
    grandcherokee     : 'grand_cherokee',
    wrangler          : 'wrangler',
    wranglerunlimited : 'wrangler',
  }
}

const COLOR_MAP = {
  black       : ['black'],
  white       : ['white'],
  silver      : ['silver', 'glacier'],
  gray        : ['gray', 'gray', 'granite', 'rhino', 'anvil', 'maximum steel', 'charcoal'],
  red         : ['red', 'burgundy', 'maroon'],
  blue        : ['blue', 'winter chill'],
  green       : ['green'],
  orange      : ['orange'],
  yellow      : ['yellow'],
  tan         : ['tan', 'beige', 'cashmere', 'mojave'],
  brownstone  : ['brownstone', 'pewter'],
  brown       : ['rugged brown']
}

const UPDATE_VEHICLE_QUERY = `
  update vehicles set 
    model = :model,
    drivetrain = :drivetrain,
    color = :color,
    estimated_value = :estimated_value,
    model_validated_on = now(),
    _owner = :owner,
    distance = :distance,
    year = :year
  where vin = :vin
`

const sanitize_model_name = function(source, model) {
  if (!MODEL_NAME_MAP[source]) throw Error(`sanitize_model_name: source not implemented: ${JSON.stringify({ source })}`)
  if (!MODEL_NAME_MAP[source][model]) throw Error(`sanitize_model_name: model not implemented for source: ${JSON.stringify({ source, model })}`)
  return MODEL_NAME_MAP[source][model]
}

const parse_drivetrain = function(raw_drivetrain) {
  switch (raw_drivetrain.toLowerCase()) {
    case '4x4':
    case 'four wheel drive':
      return '4x4'
      break
    case 'fwd':
    case 'front wheel drive':
      return 'fwd'
      break
    case 'rwd':
    case 'rear wheel drive':
      return 'rwd'
      break
    case '2wd':
    case '4x2':
      return '2wd'
      break
    default: return raw_drivetrain.toLowerCase()
  }
}

const parse_color = function(raw_color) {
  let parsed_color
  _.forEach(COLOR_MAP, (included_colors, sanitized_color) => {
    if (_.some(included_colors, incl => raw_color.toLowerCase().includes(incl))) parsed_color = sanitized_color
  })
  return parsed_color || raw_color.toLowerCase()
}

// actually retrieve vehicle data
const retrive_vehicle_data = async function({ vehicle, source }) {
  let data
  switch (source) {
    case 'autolist':
      const autolist_options = {
        url     : `${ENDPOINT_MAP[source]}/${vehicle.vin}`,
        method  : 'GET',
        timeout : TIMEOUT,
        headers : DEFAULT_HEADERS,
        qs      : { jumpstart: 'desktop' }
      }
      data = await js_utils.make_request(autolist_options)
      
      data.model = sanitize_model_name(source, data.jumpstart_info.mod)
      data.drivetrain = parse_drivetrain(data.driveline)
      data.color = parse_color(data.exterior_color)
      data.estimated_value = _.round(data.price / ((100 + data.relative_price_difference)/100))
      data.owner = data.dealer_name
      data.distance = zipcodes.distance(parseInt(data.zip), config.location_config.zip)
      data.year = data.year
      break
    case 'edmunds':
      const script_pre_json = 'window.__PRELOADED_STATE__ = '
      const script_post_json = /;$/

      const edmunds_options = {
        url     : `${ENDPOINT_MAP[source]}/${API_MAP.edmunds.model[vehicle.model]}/${vehicle.year}/vin/${vehicle.vin}`,
        method  : 'GET',
        timeout : TIMEOUT,
        headers : {
          ...DEFAULT_HEADERS,
          'accept'          : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
          'accept-encoding' : 'gzip, deflate, br',
          'accept-language' : 'en-US,en;q=0.9',
          'connection'      : 'keep-alive',
          'sec-fetch-dest'  : 'document',
          'sec-fetch-mode'  : 'navigate',
          'sec-fetch-site'  : 'none',
          'upgrade-insecure-requests': 1
        },
        gzip    : true
      }
      const response = await js_utils.make_request(edmunds_options, {skip_parse: true})
      const $ = cheerio.load(response)
      $('script').each((index, result) => {
        if ($(result).html().includes(script_pre_json)) {
          data =  JSON.parse($(result).html().replace(script_pre_json, '').replace(script_post_json, ''))
        }
      })
      data.model = vehicle.model
      data.drivetrain = parse_drivetrain(data.seo.headContent.jsonld[0].driveWheelConfiguration)
      data.color = parse_color(data.seo.headContent.jsonld[0].color)
      data.estimated_value = data.inventory.vin[vehicle.vin].thirdPartyInfo.priceValidation.listPriceEstimate
      data.owner = data.inventory.vin[vehicle.vin].dealerInfo.name
      data.distance = parseInt(data.inventory.vin[vehicle.vin].dealerInfo.distance)
      data.year = parseInt(data.pageContext.vehicle.modelYear.year)
      break
  }
  return data
}

// handle retriving data from vehicle's listed active_sources
const get_vehicle_data = async function({ vehicle }) {
  // only need to augment from a single source
  const source_preference = [
    'autolist',
    'edmunds',
    // 'auto_trader'
  ]
  
  let data, source_num = 0
  while (!data && source_num < source_preference.length) {
    if (vehicle.active_sources.includes(source_preference[source_num])) {
      data = await retrive_vehicle_data({vehicle, source: source_preference[source_num]})
    }
    source_num++
  }
  if (!data) {
    // disabled error throw b/c found it impossible to implement autotrader-based augment
    throw Error(`get_vehicle_data: no vehicle source supported: ${JSON.stringify({ vehicle })}`)
    // console.error(`get_vehicle_data: no vehicle source supported: ${JSON.stringify({ vehicle })}`)
  }
  return data
}

const update_vehicle = async function({ vehicle, data }) {
  await pg.query(UPDATE_VEHICLE_QUERY, {
    ..._.pick(vehicle, ['vin']),
    ..._.pick(data, ['model', 'drivetrain', 'color', 'estimated_value', 'owner', 'distance', 'year'])
  })
}

// augment a single vehicle
const augment = async function({ vehicle }) {
  const data = await get_vehicle_data({ vehicle })

  // const filepath = '../../../Downloads/augment_data.json'
  // fs.writeFileSync(filepath, JSON.stringify(data))
  // console.log(`wrote data to: ${filepath}`)
  // process.exit(0)

  if (data) await update_vehicle({ vehicle, data })
}

const augment_vehicles = async function({ vehicles }) {
  let augmented = 0, skipped = 0, paused = false
  await Bluebird.map(vehicles, async vehicle => {
    while (paused) {
      await Bluebird.delay(PAUSED_RETRY_DELAY)
    }
    
    try {
      await augment({vehicle})
    } catch (augment_error) {
      console.error(`augment_error, skipping: ${JSON.stringify({ augment_error: String(augment_error), vehicle })}`)
      skipped++
    }
    
    augmented++
    if (augmented % MAX_VEHICLE_RATE === 0) {
      console.log(`augmented ${augmented} / ${vehicles.length} vehicles (${JSON.stringify({ skipped })})...`)
      paused = true
      await Bluebird.delay(1000)
      paused = false
    }
  }, { concurrency: VEHICLE_CONCURRENCY })
  console.log(`...augmented all ${augmented} / ${vehicles.length} vehicles ${JSON.stringify({ skipped })})`)
}

// DONT AUGMENT all of DB until you've finalized all fields to add
const main = async function() {
  let vehicles

  // test one vehicle
  // vehicles = [{
  //   vin             : '3C4NJDBBXHT672154',
  //   active_sources  : ['edmunds'],
  //   model           : 'compass',
  //   year            : 2017
  // }]

  // revalidate all ranked listings from csv
  const RANKED_LISTINGS_PATH = 'results/ranked_listings.csv'
  const full_ranked_listings = await js_utils.read_csv(RANKED_LISTINGS_PATH)
  vehicles = _.map(full_ranked_listings, vehicle => _.pick(vehicle, ['vin', 'model', 'year', 'active_sources']))

  // validate all from query
  // vehicles = await pg.query(`
  //   with vehicle_sources as (
  //     select vehicles.vin, vehicles.model, vehicles.year, array_agg(distinct listings.source order by listings.source) as active_sources
  //     from vehicles join vehicle_listings listings
  //       on vehicles.vin = listings.vin
  //     group by vehicles.vin
  //   ) select * from vehicle_sources where ARRAY['autolist', 'edumunds'] && active_sources
  // `)
  
  await augment_vehicles({ vehicles })
}

module.exports = { 
  // augment,
  augment_vehicles
}

if (require.main === module) {
  main()
    .catch((err) => {
      console.error(err)
      process.exit(1)
    })
    .then(() => process.exit(0))
}