const faker = require('faker')
const _ = require('lodash')
const path = require('path')
const fs = require('fs')
const prettyBytes = require('pretty-bytes')

const MIN_ROWS = 1000000 // 1M
const MAX_ROWS = 1500000 // 1.5M
const BULKS_TO_GENERATE = 2

const generateBulk = (rowsCount, file) => {
  const headers = [
    'id',
    'firstName',
    'lastName',
    'phoneNumber',
    'addressStreet',
    'addressState',
    'addressCountry',
    'dateOfBirth',
    'profilePhoto',
    'username'
  ]

  fs.writeFileSync(file, headers.join(','))

  const data = _.times(rowsCount).forEach(() => {
    const row = [
      faker.random.number({ min: 1000000000, max: 2000000000 }),
      faker.name.firstName(),
      faker.name.lastName(),
      faker.phone.phoneNumber(),
      faker.address.streetAddress(),
      faker.address.state(),
      faker.address.country(),
      faker.date.past(),
      faker.image.imageUrl(),
      faker.internet.userName()
    ]

    const rowCsv = row.join('","').replace(/^/, '"').replace(/$/, '"')

    fs.writeFileSync(file, `\n${rowCsv}`, { flag: 'a+' })
  })

  return fs.statSync(file).size
}

_.times(BULKS_TO_GENERATE).forEach((bulkNumber) => {
  const rowsCount = faker.random.number({ min: MIN_ROWS, max: MAX_ROWS })
  const fileBulk = path.join(__dirname, `bulk-${bulkNumber + 1}.json`)

  console.log(`Generating bulk #${bulkNumber + 1}: ${rowsCount} rows`)

  const sizeBulk = generateBulk(rowsCount, fileBulk)
  console.log(`Bulk generated in ${fileBulk} (${prettyBytes(sizeBulk)})`)
})
