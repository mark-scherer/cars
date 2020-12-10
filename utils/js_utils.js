'use strict'

const _         = require('lodash')
const fs        = require('fs')
const csv       = require('csv-parser')
const stringify = require('csv-stringify')
const request   = require('request')

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

const make_request = (options) => {
  return new Promise((resolve, reject) => {
    request(options, (error, response, body) => {
      if (error) return reject(error)
      if (response.statusCode !== 200) return reject(`non-200 statusCode: ${response.statusCode}`)
      
      return resolve(JSON.parse(body))
    })
  })
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
  require_args
}