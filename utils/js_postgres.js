/*
  JS Postgres util

  uses yesql.pg for parsing query output
*/

'use strict'

const { Pool }  = require('pg')
const named     = require('yesql').pg

let pools = {}

const connect = function(config, name='default') {
  pools[name] = new Pool(config)
  pools[name].on('error', (err, client) => {
    console.error(`Postgres util: unexpected error on idle client ${name}: ${err}`)
    process.exit(-1)
  })

  return new Promise((resolve, reject) => {
    pools[name].connect((err, client) => {
      if (err) return reject(err)
      return resolve()
    })
  })
}

const query = function(query_str, data, name='default') {
  if (!pools[name]) throw Error(`Postgres util: haven't yet setup connection: ${name}`)
  
  return new Promise((resolve, reject) => {
    let formatted_query
    try {
      formatted_query = named(query_str, { useNullForMissing: true })(data)
    } catch (parse_error) {
      return reject(`Postgres util: error parsing query: ${JSON.stringify({
        parse_error: String(parse_error),
        query_str,
        data
      })}`)
    }
    pools[name].query(formatted_query.text, formatted_query.values, (err, result) => {
      if (err) return reject(`Postgres util: error running query: ${JSON.stringify({
        error: String(err),
        query_str,
        data
      })}`)
      return resolve(result.rows)
    })
  })
}

const one = function(query_str, data, name='default') {
  return new Promise((resovle, reject) => {
    const result = query(query_str, data, name)
      .catch((err) => reject(err))
      .then((result) => {
        if (result.length !== 1) return reject(`Postgres util: query did not return single row (${result.length}): ${query_str}`)
        return resolve(result[0])
      })
  })
}


module.exports = {
  connect,
  query
}