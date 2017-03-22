# promises-blob-store

```
npm install promise-blob-store
```

[![build status](http://img.shields.io/travis/mafintosh/fs-blob-store.svg?style=flat)](http://travis-ci.org/mafintosh/fs-blob-store)
![dat](http://img.shields.io/badge/Development%20sponsored%20by-dat-green.svg?style=flat)

## Usage

``` js
var fs = require('promise-blob-store')
var blobs = fs('some-directory')

var ws = blobs.createWriteStream({
  key: 'some/path/file.txt'
})

ws.write('hello world\n')
ws.end(function() {
  var rs = blobs.createReadStream({
    key: 'some/path/file.txt'
  })

  rs.pipe(process.stdout)
})
```

## License

MIT
