import de.aonnet.gcouch.GroovyCouchDb
import groovy.json.JsonOutput
import groovyx.net.http.RESTClient

import static groovyx.net.http.ContentType.JSON

// connection data
String host = 'localhost'
String couchdbName = 'elastictest'

// new couchdb with a object
GroovyCouchDb couchDb = new GroovyCouchDb(host: host, dbName: couchdbName)
//couchDb.cleanDb()
//couchDb.create([type: 'test', greeding: 'hello', name: 'thomas'])
//couchDb.create([type: 'test', greeding: 'hello', name: 'heike'])

// elasticsearch rest connection
RESTClient elasticsearch = new RESTClient("http://${host}:9200")

// map to create a couchdb river
Map createCouchDbRiverMap = [
        'type': 'couchdb',
        'couchdb': [
                'host': host,
                'port': 5984,
                'db': couchdbName,
                'filter': null
        ]
        , 'index': [
        'index': couchdbName,
        'type': couchdbName,
        'bulk_size': '100',
        'bulk_timeout': '10ms'
]]
String createCouchDbRiverJson = JsonOutput.toJson(createCouchDbRiverMap)

def response

// create the couchdb river
//response = elasticsearch.put(path: "_river/${couchdbName}/_meta", contentType: JSON, requestContentType: JSON, body: createCouchDbRiverJson)
//println response.data
//assert response.status == 200 || response.status == 201
//assert response.data.ok: 'Es ist ein Fehler aufgetreten!'
//assert response.data._index == '_river'
//assert response.data._type == couchdbName

// check couchdb river status
response = elasticsearch.get(path: "_river/${couchdbName}/_status", requestContentType: JSON)
println response.data
assert response.status == 200
assert response.data._index == '_river'
assert response.data._type == couchdbName
assert response.data.exists == true

// run a elasticsearch query
Map query = [q: 'name:thomas', pretty: 'true']
response = elasticsearch.get(path: "${couchdbName}/_search", query: query, requestContentType: JSON)
println response.data
assert response.status == 200
assert response.data.timed_out == false
assert response.data.hits.total == 1
Map hit = response.data.hits.hits.get(0)
assert hit._index == couchdbName
assert hit._type == couchdbName
assert hit._source.greeding == 'hello'
assert hit._source.name == 'thomas'
assert hit._source.type == 'test'

query = [q: 'name:heike', pretty: 'true']
response = elasticsearch.get(path: "${couchdbName}/_search", query: query, requestContentType: JSON)
println response.data
assert response.status == 200
assert response.data.timed_out == false
assert response.data.hits.total == 1
hit = response.data.hits.hits.get(0)
assert hit._index == couchdbName
assert hit._type == couchdbName
assert hit._source.greeding == 'hello'
assert hit._source.name == 'heike'
assert hit._source.type == 'test'

query = [q: 'greeding:hello', pretty: 'true']
response = elasticsearch.get(path: "${couchdbName}/_search", query: query, requestContentType: JSON)
println response.data
assert response.status == 200
assert response.data.timed_out == false
assert response.data.hits.total == 2
response.data.hits.hits.each {
    assert it._index == couchdbName
    assert it._type == couchdbName
    assert it._source.greeding == 'hello'
    assert it._source.type == 'test'
}


query = [q: 'type:test', pretty: 'true']
response = elasticsearch.get(path: "${couchdbName}/_search", query: query, requestContentType: JSON)
println response.data
assert response.status == 200
assert response.data.timed_out == false
assert response.data.hits.total == 2
response.data.hits.hits.each {
    assert it._index == couchdbName
    assert it._type == couchdbName
    assert it._source.greeding == 'hello'
    assert it._source.type == 'test'
}

// query dsl
query = ['text': ['type': 'test']]
response = elasticsearch.get(path: "${couchdbName}/_search", query: query, requestContentType: JSON)
println response.data
assert response.status == 200
assert response.data.timed_out == false
assert response.data.hits.total == 2
response.data.hits.hits.each {
    assert it._index == couchdbName
    assert it._type == couchdbName
    assert it._source.greeding == 'hello'
    assert it._source.type == 'test'
}
