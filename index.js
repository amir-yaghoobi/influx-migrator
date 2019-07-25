const Influx = require('influx')
const inquirer = require('inquirer')
const ora = require('ora')
const yargs = require('yargs')
const fs = require('fs')
const path = require('path')
const os = require('os')
const Promise = require('bluebird')
const lowdb = require('lowdb')
const FileSync = require('lowdb/adapters/FileSync')
const _ = require('lodash')

global.Promise = Promise

const adapter = new FileSync('.data.json')

const db = lowdb(adapter)

db.defaults({
  state: {}
}).write()

const argv = yargs
  .usage(`Usage: $0 --source 127.0.0.1:8086 --destination 127.0.0.1:9086`)
  .alias('s', 'source')
  .alias('d', 'destination')
  .alias('p', 'pattern')
  .alias('c', 'clean')
  .alias('h', 'help')
  .describe('s', 'Influx source database')
  .describe('d', 'Influx destination database')
  .describe('p', 'Regex pattern to filter databases')
  .describe('c', 'Execute program with clean state')
  .demandOption(['s', 'd'])
  .help('h').argv

async function migrateCommand(source, destination, pattern) {
  const [srcHost, srcPort] = source.split(':')
  const [destHost, destPort] = destination.split(':')

  if (!srcHost || !srcPort) {
    ora().fail('Invalid source influx database format (please provide host:port)')
    return yargs.showHelp()
  }

  if (!destHost || !destPort) {
    ora().fail('Invalid destination influx database format (please provide host:port)')
    return yargs.showHelp()
  }

  const srcInflux = new Influx.InfluxDB({
    database: 'adppush_dev',
    host: srcHost,
    port: srcPort
  })
  const destInflux = new Influx.InfluxDB({
    database: 'adppush_dev',
    host: destHost,
    port: destPort
  })

  const loadingDbOra = ora({
    text: 'Loading source databases',
    spinner: 'dots'
  }).start()

  try {
    let sourceDatabases = await srcInflux.query('SHOW DATABASES')
    const initialCount = sourceDatabases.length
    if (pattern) {
      sourceDatabases = sourceDatabases.filter(({ name }) => name.match(pattern))
    }
    sourceDatabases = sourceDatabases.map(({ name }) => name)

    const state = db.get('state').value()

    loadingDbOra.succeed(`loaded [${sourceDatabases.length}/${initialCount}] databases`)

    await Promise.each(sourceDatabases, async database => {
      const databaseProgress = ora(`Processing "${database}"`).start()

      if (!state[database]) {
        state[database] = {}
        db.set('state', state).write()
      }

      const measurements = await srcInflux.query(`SHOW MEASUREMENTS ON "${database}"`)

      await destInflux.createDatabase(database)

      databaseProgress.succeed('Database created on destination')

      databaseProgress.start('Start migrating measurements')

      await Promise.each(measurements, async ({ name: measurement }) => {
        databaseProgress.start('Migrating measurement: ' + measurement)

        if (state[database][measurement] === true) {
          return databaseProgress.info(`Skip "${database}"."${measurement}"`)
        }

        try {
          const resultPoints = await srcInflux.query(`SELECT * FROM "${measurement}"`, {
            database
          })

          const writePoints = resultPoints.map(point => {
            const fields = Object.keys(point).reduce((p, f) => {
              if (f !== 'time' && point[f] !== null) {
                p[f] = point[f]
              }
              return p
            }, {})

            return {
              timestamp: point.time,
              measurement,
              fields
            }
          })

          const chunks = _.chunk(writePoints, 5000)

          await Promise.map(chunks, points => {
            return destInflux.writePoints(points, { database })
          })

          state[database][measurement] = true
          db.set('state', state).write()

          databaseProgress.succeed()
        } catch (err) {
          state[database][measurement] = err.message
          db.set('state', state).write()

          databaseProgress.warn(`error during migrating "${database}"."${measurement}"`)
        }
      })

      databaseProgress.succeed(`Migrating database: ${database} completed`)
    })
  } catch (err) {
    loadingDbOra.fail(err.message)
  }
}

async function start() {
  if (argv.c) {
    const cleanUpOra = ora('Cleaning state')

    // try {
    //   await Promise.fromCallback(cb => fs.unlink(DATA_DIR, cb))
    //   cleanUpOra.succeed()
    // } catch (err) {
    //   if (err && err.code !== 'ENOENT') {
    //     return cleanUpOra.fail(err.message)
    //   } else {
    //     cleanUpOra.succeed()
    //   }
    // }
  }

  // const stateString = await Promise.fromCallback(cb =>
  //   fs.readFile(DATA_DIR, { encoding: 'utf8' }, cb)
  // )

  // console.log('>>>', stateString)

  await migrateCommand(argv.s, argv.d, argv.p)
}

start()
