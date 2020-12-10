/*
  For importing existing csv to postgres tables

  NOTE: not updated to various util changes
*/

'use strict'

const _               = require('lodash')
const bluebird        = require('Bluebird')
const config          = require('../incl/config')
const js_utils        = require('../utils/js_utils')
const pg              = require('../utils/js_postgres')
const argv            = require('minimist')(process.argv.slice(2))

const FILEPATH        = argv.filepath
const MODEL           = argv.model
const SOURCE          = argv.source
const ROW_CONCURRENCY = argv.concurrency || 8
const VERBOSE         = argv.verbose
const REQUIRED_ARGS   = ['filepath', 'model', 'source']

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

pg.connect(config.pg_config)

const main = async function() {
  throw Error(`not yet updated to various util changes`)

  js_utils.require_args(argv, REQUIRED_ARGS)
  if (!(MODEL in config.naming_configs)) throw Error(`unknown model: ${model}`)

  const data = await js_utils.read_csv(FILEPATH)

  let processed = 0
  await bluebird.map(data, async row => {
    // insert vehicle if not alread in table
    try {
      await pg.query(INSERT_VEHICLE_QUERY, {
        make    : config.naming_configs[MODEL].make,
        model   : config.naming_configs[MODEL].model,
        version : row.trim,
        ..._.pick(row, ['vin', 'year'])
      })
    } catch (pg_error) {
      if (String(pg_error).includes(PG_DUPE_ERROR)) {
        if (VERBOSE) console.log(`found duped vehicle, skipping: ${row.vin}`)
      }
      else throw Error(`insert vehicle pg_error: ${pg_error}`)
    }

    // insert listing
    const date_string = new Date(row.scrape_time).toUTCString()
    try {
      await pg.query(INSERT_LISTING_QUERY, {
        scrape_time : date_string,
        source      : SOURCE,
        owner       : row.ownerName,
        mileage     : row.mileage   || 0,
        ..._.pick(row, ['vin', 'zip', 'price', 'title'])
      })
    } catch (pg_error) {
      if (String(pg_error).includes(PG_DUPE_ERROR)) {
        if (VERBOSE) console.log(`found duped listing, skipping: ${JSON.stringify(_.pick(row, ['vin', 'scrape_time']))}`)
      }
      else throw Error(`insert vehicle pg_error: ${pg_error}`)
    }
    

    processed++
    if (processed % 10 === 0) console.log(`processed ${processed} / ${data.length} rows`)
  }, { concurrency: ROW_CONCURRENCY})
  console.log(`finshed processing ${data.length} rows from ${FILEPATH}`)
}

main()
  .catch((err) => {
    console.error(err)
    process.exit(1)
  })
  .then(() => process.exit(0))
