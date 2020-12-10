/*
  Handler for running scrape inside aws lambda
*/

const scrape = require('./scripts/scrape')

const handler = async function(event, context) {
  const diagnostics = await scrape.scrape_and_log()
  console.log(`scrape_and_log diagnostics: ${JSON.stringify({ diagnostics })}`)
  return diagnostics
}
exports.handler = handler

if (require.main === module) {
  handler()
    .catch((err) => {
      console.error(err)
      process.exit(1)
    })
    .then(() => process.exit(0))
}