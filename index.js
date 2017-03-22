const mkdirp = require('mkdirp')
const LRU = require('lru-cache')
const duplexify = require('duplexify')
const path = require('path')
const fs = require('fs-promise')

let noop = function() {}

let join = function(root, dir) {
  return path.join(root, path.resolve('/', dir).replace(/^[a-zA-Z]:/, ''))
}

let BlobStore = function(opts) {
  if (!(this instanceof BlobStore)) return new BlobStore(opts)
  if (typeof opts === 'string') opts = {path:opts}

  this.path = opts.path
  this.cache = LRU(opts.cache || 100)
}

BlobStore.prototype.createWriteStream = function(opts) {
  if (typeof opts === 'string') opts = { key:opts }
  if (opts.name && !opts.key) opts.key = opts.name

  let key = join(this.path, opts.key)
  let dir = path.dirname(key)
  let cache = this.cache

  if (cache.get(dir)) return fs.createWriteStream(key, opts)

  let proxy = duplexify()

  proxy.setReadable(false)

  mkdirp(dir, function(err) {
    if (proxy.destroyed) return
    if (err) return proxy.destroy(err)
    cache.set(dir, true)
    proxy.setWritable(fs.createWriteStream(key, opts))
  })

  return proxy
}

BlobStore.prototype.createReadStream = function(key, opts) {
  if (key && typeof key === 'object') return this.createReadStream(key.key, key)
  return fs.createReadStream(join(this.path, key), opts)
}

BlobStore.prototype.exists = function(opts, cb) {
  if (typeof opts === 'string') opts = {key:opts}
  let key = join(this.path, opts.key)
  return fs.stat(key)
}

BlobStore.prototype.remove = function(opts, cb) {
  if (typeof opts === 'string') opts = {key:opts}
  if (!opts) opts = noop
  let key = join(this.path, opts.key)
  return fs.unlink(key)
}

module.exports = BlobStore
