'use strict'

module.exports = (config) => {
  return msg(this.config)
    .then(({ globals }) => {
      global.Message  = globals.Message
      global.Listener = globals.Listener
    })
}
