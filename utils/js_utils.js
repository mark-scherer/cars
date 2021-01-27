'use strict'

const _         = require('lodash')
const Bluebird  = require('bluebird')
const fs        = require('fs')
const csv       = require('csv-parser')
const stringify = require('csv-stringify')
const request   = require('request')
// const puppeteer = require('puppeteer')

const DEFAULT_TIMEOUT = 10000 // ms

const read_csv = (filepath) => {
  let data = []
  return new Promise((resolve, reject) => {
    const in_stream = fs.createReadStream(filepath).pipe(csv())

    in_stream.on('data', row => {
      data.push(row)
    })

    in_stream.on('end', () => {
      console.log(`read ${data.length} rows from ${filepath}`)
      return resolve(data)
    })
  })
}

const write_csv = (filepath, data) => {
  return new Promise((resolve, reject) => {
    stringify(data, {
      header: true
    }, (err, output) => {
        if (err) return reject(err)
        fs.writeFileSync(filepath, output)
        console.log(`wrote ${data.length} rows to ${filepath}`)
        return resolve()
    })
  })
}

const make_request = function(request_options, other_options={}) {
  return new Promise((resolve, reject) => {
    request(request_options, (error, response, body) => {
      if (error) return reject(error)
      if (response.statusCode !== 200) return reject(`non-200 statusCode: ${JSON.stringify({..._.pick(response, ['statusCode']), request_options})}`)

      if (other_options.skip_parse) return resolve(body)

      try { 
        return resolve(JSON.parse(body)) 
      } catch (parse_error) { 
        return reject({
          error     : `response parse error: ${parse_error}`,
          body 
        }) 
      }
    })
  })
}

// options:
  // verbose      : console log status updates
  // timeout
  // user_agent   : user agent to use in request
  // wait_until   : run puppeteer.waitForNavigation() after load. Values:
                    // load             : load event
                    // domcontentloaded : DOMContentLoaded event
                    // networkidle0     : 0 network connections for 500ms
                    // networkidle2     : <=2 network connections for 500ms
  // detect_denied: LIST of strings, will throw error if any detected in response text
  // stay_on_page : stay on page after load for x ms
const make_headless_request = async function(url, options) {
  throw Error(`puppeteer removed, was too big to run in lambda!`)

  if (!options.timeout) options.timeout = DEFAULT_TIMEOUT

  const browser = await puppeteer.launch()
  const page = await browser.newPage()
  if (options.user_agent) await page.setUserAgent(options.user_agent)

  let response
  try {
    response = await page.goto(url, {
      waitUntil: options.wait_until, 
      timeout: options.timeout
    })

    if (options.detect_denied) {
      const text = await response.text()
      const detected = []
      _.forEach(options.detect_denied, async denial_message => {
        if (text.includes(denial_message)) detected.push(denial_message)
      })
      if (detected.length > 0) throw Error(`denial_detected: ${detected}`)
    }
  } catch (page_load_error) {
    console.error(`js_utils: make_headless_request: page_load_error: ${JSON.stringify({ url, options, page_load_error: String(page_load_error) })}`)
  }
  
  if (options.verbose) console.log(`js_utils: make_headless_request: page load finished: ${JSON.stringify({url})}`)
  if (options.stay_on_page) await Bluebird.delay(options.stay_on_page)

  await browser.close()
}

const require_args = function(argv, arg_list) {
  _.forEach(arg_list, required_arg => {
    if (!argv[required_arg] && argv[required_arg] !== 0) throw Error(`missing required arg: ${required_arg}`)
  })
}

module.exports = {
  read_csv,
  write_csv,
  make_request,
  make_headless_request,
  require_args
}