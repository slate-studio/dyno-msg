'use strict'

const _ = require('lodash')

const RequestNamespace = require('@slatestudio/dyno/lib/requestNamespace')

class Msg {
  constructor(channel, json) {
    this.channel = channel
    const source = JSON.parse(json)

    this.object  = source.object
    this.headers = source.headers
  }

  async exec(callback) {
    const requestId           = _.get(this.headers, 'requestId', null)
    const authenticationToken = _.get(this.headers, 'authenticationToken', null)
    const facilityScope       = _.get(this.headers, 'facilityScope', null)
    const sourceOperationId   = _.get(this.headers, 'sourceOperationId', null)

    if (!authenticationToken) {
      log.warn('[msg] authenticationToken header is not defined, skiping message')
      return
    }

    if (!facilityScope) {
      log.warn('[msg] facilityScope header is not defined, skiping message')
      return
    }

    const namespace = {
      requestId,
      authenticationToken,
      facilityScope,
      sourceOperationId
    }

    this.requestNamespace = new RequestNamespace(namespace)
    this.requestNamespace.save([], async() => {
      log.info(`[msg] Got message from ${this.channel}`)

      try {
        await callback(this)
        log.info('[msg] Message succesfully handled')

      } catch (error) {
        log.error('[msg] Message handler error:', error)

      }
    })
  }
}

module.exports = Msg
