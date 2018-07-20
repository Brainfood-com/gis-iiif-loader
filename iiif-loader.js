#!/usr/bin/env node

const { Client, Pool } = require('pg')

const assert = require('assert')
const fs = require('fs')
const zlib = require('zlib')

const imageSizes = {}
const allData = {}

const assertsByType = {
  'sc:Collection': {
    '@context': 'http://iiif.io/api/presentation/2/context.json',
  },
  'sc:Manifest': {
    '@context': 'http://iiif.io/api/presentation/2/context.json',
  },
}

function incr(store, key) {
  store[key] = (store[key] || 0) + 1
}

/*
 * CREATE TABLE content (
 *  id SERIAL,
 *  type TEXT,
 * )
 */
function dbAdd({table}) {

}

function jsonFileParser(file) {
  return new Promise((resolve, reject) => {
    const fileStream = fs.createReadStream(file)
    const parts = []
    const stream = (file.match(/\.gz$/) ? fileStream.pipe(zlib.createGunzip()) : fileStream)
    stream.on('error', error => {
      console.log('error', error)
      reject(error)
    })
    stream.on('data', data => {
      parts.push(data.toString())
    })
    stream.on('end', () => resolve(JSON.parse(parts.join(''))))
  })
}

const labelFixup = /^Edward Ruscha photographs of ((Los Angeles streets|Sunset Boulevard): )?/

class Parser {
  constructor(props) {
    this.allResults = []
    this.rowsByType = {}
    this.columnCountsByKey = {}
    this.externalIdLookup = {}
    this._pgConnection = new Client({
			user: 'gis',
			host: 'postgresql',
			database: 'gis',
			password: 'sig',
//			port: 3211,
    })
    this._pgConnection.connect()
    this.startup = Promise.all([
      this._pgConnection.query('SELECT iiif_id, iiif_type_id, external_id FROM iiif WHERE external_id IS NOT NULL').then(result => {
        //console.log('result', result)
        result.rows.forEach(row => {
          const {external_id, iiif_id, iiif_type_id} = row
          this.externalIdLookup[external_id] = iiif_id
        })
      }).catch(err => {
        console.log('err', err)
      }),
    ])
    this.extensionTables = {
      'iiif_canvas': ['iiif', 'iiif_id'],
      'iiif_manifest': ['iiif', 'iiif_id'],
      'iiif_range': ['iiif', 'iiif_id'],
    }
  }

  parseFile(file) {
    const result = jsonFileParser(file).then(json => {
      return this.startup.then(() => this.parse(json))
    })
    this.allResults.push(result)
    return result
  }

  parse(json, owner) {
    const {'@type': parseType} = json
    assert(parseType.indexOf(':') !== -1)
    const parser = this[parseType] || this[null]
    const result = parser.call(this, json, {parseType, owner})
    result.then(result => {
      const {'@id': id, '@type': type} = result
      const byType = this.rowsByType[type] || (this.rowsByType[type] = {})
      assert.deepEqual(!!byType[id], false)
      byType[id] = result
    })
    return result
  }

  async parseAll(list = [], owner) {
    const result = new Array(list.length)
    for (const item of list) {
      result.push(await this.parse(item, owner))
    }
    return result
  }

  addRow(table, pk, row) {
    const pkKeys = Object.keys(pk)
    const rowKeys = Object.keys(row)
    const allKeys = [].concat(pkKeys, rowKeys)
    const valueListParams = []
    const updateListParams = []
    const keyToValueMap = {}
    let keyCount = 0
    const values = []
    pkKeys.forEach(key => {
      values.push(pk[key])
      const count = ++keyCount
      keyToValueMap['pk:' + key] = count
      valueListParams.push('$' + count)
    })
    rowKeys.forEach(key => {
      const value = row[key]
      values.push(key === 'label' ? value.replace(labelFixup, '') : value)
      const count = ++keyCount
      keyToValueMap['row:' + key] = count
      valueListParams.push('$' + count)
      updateListParams.push('$' + count)
    })
    const query = `INSERT INTO ${table} (${allKeys.join(', ')}) VALUES (${valueListParams}) ON CONFLICT (${pkKeys.join(', ')}) DO UPDATE SET (${rowKeys.join(', ')}) = ROW(${updateListParams})`
    return this._pgConnection.query(query, values).catch(err => {
      console.error(query)
      console.error(err)
      throw err
    })
  }

  async getId(externalId) {
    //console.log('getId', externalId)
    const result = this.externalIdLookup[externalId]
    if (result) {
      return result
    }
    const findQuery = {
      text: 'SELECT iiif_id FROM iiif WHERE external_id = $1',
      values: [externalId],
    };
    const findResult = await this._pgConnection.query(findQuery)
    if (findResult.rowCount) {
      return this.externalIdLookup[externalId] = findResult.rows[0].iiif_id
    }
    const insertResult = await this._pgConnection.query('INSERT INTO iiif(external_id) VALUES($1) ON CONFLICT(external_id) DO NOTHING RETURNING iiif_id', [externalId])
    if (insertResult.rowCount) {
        return this.externalIdLookup[externalId] = insertResult.rows[0].iiif_id
    }
    return this._pgConnection.query(findQuery).then(result => {
      return this.externalIdLookup[externalId] = result.rows[0].iiif_id
    })
  }

  finish() {
    return Promise.all(this.allResults).then(() => {
      Object.keys(this.rowsByType).sort().forEach(type => {
        console.log(`${type}:`, Object.keys(this.rowsByType[type]).length)
      })
      Object.keys(this.columnCountsByKey).sort().forEach(column => {
        console.log(`${column}: ${this.columnCountsByKey[column]}`)
      })
    })
  }

  async 'oa:Annotation'(json, {owner}) {
    const {
      '@type': type,
      motivation,
      resource: {
        '@id': id,
        '@type': resourceType,
        format,
        height,
        width,
        service: {
          '@context': serviceContext,
          '@id': service,
          profile: serviceProfile,
          ...serviceRest
        },
        ...resourceRest
      },
      on,
      ...rest
    } = json
    assert.deepEqual({
      type,
      motivation,
      resourceType,
      serviceContext,
      serviceProfile,
      serviceRest,
      resourceRest,
      on,
      rest,
    }, {
      type: 'oa:Annotation',
      motivation: 'sc:painting',
      resourceType: 'dctypes:Image',
      serviceContext: 'http://iiif.io/api/image/2/context.json',
      serviceProfile: 'http://iiif.io/api/image/2/level2.json',

      serviceRest: {},
      resourceRest: {},
      on: owner['@id'],
      rest: {},
    })
    incr(imageSizes, `${width}:${height}`)
    return {'@id': id, '@type': type, format, width, height, service}
    //return {'@table': 'iiif', '@id': id, external_id: id, type: resourceType}
  }

  async 'sc:Canvas'(json) {
    //console.log('sc:Canvas')
    const {
      '@id': id,
      '@type': type,
      label,
      viewingHint,
      thumbnail: {
        '@id': thumbnailId,
        '@type': thumbnailType,
        service: {
          '@context': thumbnailServiceContext,
          '@id': thumbnailService,
          profile: thumbnailProfile,
          ...thumbnailServiceRest
        },
        ...thumbnailRest
      },
      height,
      width,
      images,
      ...rest
    } = json
    assert.deepEqual({
      type,
      //thumbnailType,
      thumbnailServiceContext,
      thumbnailProfile,
      thumbnailServiceRest,
      thumbnailRest,
      rest,
    }, {
      type: 'sc:Canvas',
      //thumbnailType: 'dctypes:Image',
      thumbnailServiceContext: 'http://iiif.io/api/image/2/context.json',
      thumbnailProfile: 'http://iiif.io/api/image/2/level1.json',
      thumbnailServiceRest: {},
      thumbnailRest: {},
      rest: {},
    })
    incr(imageSizes, `${width}:${height}`)
    const pgId = await this.getId(id)
    const parsedImages = await Promise.all(images.map(image => this.parse(image, json)))
    assert.equal(parsedImages.length, 1)
    const parsedImage = parsedImages[0]
    assert.deepEqual({width, height}, {width: parsedImage.width, height: parsedImage.height})
    const {format, service: imageService} = parsedImage
    await this.addRow('iiif', {iiif_id: pgId}, {iiif_type_id: type, label})
    await this.addRow('iiif_canvas', {iiif_id: pgId}, {format, height, image: imageService, thumbnail: thumbnailService, width})
    return {'@table': 'iiif_canvas', '@type': type, '@id': id, label, width, height, thumbnail: thumbnailService, format, image: imageService}
  }

  async 'sc:Collection'(json) {
    const {
      '@context': context,
      '@id': id,
      '@type': type,
      label,
      members,
      ...rest
    } = json
    assert.deepEqual(rest, {}, '')
    const pgId = await this.getId(id)
    await this.addRow('iiif', {iiif_id: pgId}, {iiif_type_id: type, label})
    await Promise.all(members.map(async (member, index) => {
      const memberId = await this.getId(member['@id'])
      return this.addRow('iiif_assoc', {iiif_id_from: pgId, iiif_id_to: memberId, iiif_assoc_type_id: member['@type']}, {sequence_num: index})
    }))
    return {'@table': 'iiif', '@type': type, '@id': id, label}
  }

  async 'sc:Manifest'(json) {
    const {
      '@context': context,
      '@id': id,
      '@type': type,
      label,
      metadata,
      description,
      attribution,
      license,
      logo,
      viewingHint,
      sequences,
      structures,
      ...rest
    } = json
    assert.deepEqual(rest, {})
    const pgId = await this.getId(id)
    await this.addRow('iiif', {iiif_id: pgId}, {iiif_type_id: type, label})
    await Promise.all((await this.parseAll(sequences)).map(async (member, index) => {
      const memberId = await this.getId(member['@id'])
      return this.addRow('iiif_assoc', {iiif_id_from: pgId, iiif_id_to: memberId, iiif_assoc_type_id: member['@type']}, {sequence_num: index})
    }))
    await Promise.all((await this.parseAll(structures)).map(async (member, index) => {
      const memberId = await this.getId(member['@id'])
      return this.addRow('iiif_assoc', {iiif_id_from: pgId, iiif_id_to: memberId, iiif_assoc_type_id: member['@type']}, {sequence_num: index})
    }))
    await this.addRow('iiif_manifest', {iiif_id: pgId}, {description, attribution, license, logo, viewing_hint: viewingHint})
    return {'@table': 'iiif_manifest', '@type': type, '@id': id, label, description, attribution, license, logo, viewingHint}
  }

  async 'sc:Range'(json) {
    const {
      '@id': id,
      '@type': type,
      label,
      viewingHint,
      ranges = [],
      canvases = [],
      ...rest
    } = json
    assert.deepEqual(rest, {})
    const pgId = await this.getId(id)
    await this.addRow('iiif', {iiif_id: pgId}, {iiif_type_id: type, label})
    await this.addRow('iiif_range', {iiif_id: pgId}, {viewing_hint: viewingHint})
    await Promise.all(ranges.map(async (range, index) => {
      const memberId = await this.getId(range)
      return this.addRow('iiif_assoc', {iiif_id_from: pgId, iiif_id_to: memberId, iiif_assoc_type_id: 'sc:Range'}, {sequence_num: index})
    }))
    await Promise.all(canvases.map(async (canvas, index) => {
      const memberId = await this.getId(canvas)
      return this.addRow('iiif_assoc', {iiif_id_from: pgId, iiif_id_to: memberId, iiif_assoc_type_id: 'sc:Canvas'}, {sequence_num: index})
    }))
    return {'@table': 'iiif_range', '@type': type, '@id': id, label, viewingHint}
  }

  async 'sc:Sequence'(json) {
    const {
      '@id': id,
      '@type': type,
      label,
      canvases,
      ...rest
    } = json
    assert.deepEqual(rest, {})
    console.log('sc:sequence')
    //console.log('foo', sequences.length, structures.length)
    const pgId = await this.getId(id)
    await this.addRow('iiif', {iiif_id: pgId}, {iiif_type_id: type, label})
    await Promise.all((await this.parseAll(canvases)).map(async (member, index) => {
      const memberId = await this.getId(member['@id'])
      return this.addRow('iiif_assoc', {iiif_id_from: pgId, iiif_id_to: memberId, iiif_assoc_type_id: member['@type']}, {sequence_num: index})
    }))
    return {'@table': 'iiif', '@type': type, '@id': id, label}
  }

  async null(json, type) {
    throw new Error('unknown type: ' + type)
  }
}

const parser = new Parser()

parser.startup.then(() => {
  process.argv.slice(2).map(async file => await parser.parseFile(file))
  parser.finish().then(() => {
    process.exit(0)
  }).catch(error => {
    console.log('error', error)
    process.exit(1)
  })
}).catch(err => {
  console.error('foo')
  console.error(err)
})
