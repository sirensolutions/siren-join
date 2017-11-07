:warning: This project (Siren "Join") is superseded  by the new Siren "FEDERATE" plugin (AKA Vanguard). 

Siren Federate is capable of fully distributed (scale with the number of machines) Elasticsearch joins and can even perform joins across multiple backends making JDBC datasources appear as if they were Elasticsearch indexes. 

Siren Federate is available for Elasticsearch 5.x, and soon 6.x

For more information and downloads see http://siren.io 



# (Superseded) The SIREn Join Plugin for Elasticsearch 2.x

This plugin extends Elasticsearch with new search actions and a filter query parser that enables to perform
a "Filter Join" between two set of documents (in the same index or in different indexes).

The Filter Join is basically a (left) semi-join between two set of documents based on a common attribute, where
the result only contains the attributes of one of the joined set of documents. This join is
used to filter one document set based on a second document set, hence its name. It is equivalent
to the `EXISTS()` operator in SQL.

## Compatibility

The following table shows the compatibility between releases of Elasticsearch and the SIREn Join plugin:

Elasticsearch|SIREn Join
---|---
2.4.5|2.4.5
2.4.4|2.4.4
2.4.3|2.4.3
2.4.2|2.4.2-1
2.4.1|2.4.1-1
2.3.5|2.3.5-1
2.3.4|2.3.4-1
2.3.3|2.3.3-1
2.2.0|2.2.0-1
2.1.2|2.1.2
2.1.1|2.1.1
1.7.x|1.0

## Installing the Plugin

### Online Download

You can use the following command to download the plugin from the online repository:

    $ bin/plugin install solutions.siren/siren-join/2.4.4

### Offline Download

- Get the ZIPball from [maven.org](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22solutions.siren%22%20AND%20a%3A%22siren-join%22)
- Install with the downloaded file

    $ bin/plugin install file:/path/to/folder/with/siren-join-2.4.4.zip

### Manual

Alternatively, you can assemble it via Maven (you must build it as a *non-root* user):

```
$ git clone git@github.com:sirensolutions/siren-join.git
$ cd siren-join
$ mvn package
```

This creates a single Zip file that can be installed using the Elasticsearch plugin command:

    $ bin/plugin install file:/PATH-TO-SIRENJOIN-PROJECT/target/releases/siren-join-2.4.4.zip

### Interacting with the Plugin

You can now start Elasticsearch and see that our plugin gets loaded:

    $ bin/elasticsearch
    ...
    [2013-09-04 17:33:27,443][INFO ][node    ] [Andrew Chord] initializing ...
    [2013-09-04 17:33:27,455][INFO ][plugins ] [Andrew Chord] loaded [siren-join], sites []
    ...

To uninstall the plugin:

    $ bin/plugin remove siren-join

## Usage

### Coordinate Search API

This plugin introduces two new search actions, `_coordinate_search` that replaces the `_search` action, 
and `_coordinate_msearch` that replaces the `_msearch` action. Both actions are wrappers around the original
elasticsearch actions and therefore supports the same API. One must use these actions with the `filterjoin` filter,
as the `filterjoin` filter is not supported by the original elaticsearch actions.
 
### Parameters

* `filterjoin`: the filter name
* `indices`:  the index names to lookup the terms from (optional, default to all indices).
* `types`: the index types to lookup the terms from (optional, default to all types).
* `path`: the path within the document to lookup the terms from.
* `query`: the query used to lookup terms with.
* `orderBy`: the ordering to use to lookup the maximum number of terms: default, doc_score (optional, default to default ordering).
* `maxTermsPerShard`: the maximum number of terms per shard to lookup (optional, default to all terms).
* `termsEncoding`: the encoding to use when transferring terms across the network: long, integer, bloom, bytes (optional, default to long).

### Example

In this example, we will join all the documents from `index1` with the documents of `index2`. 
The query first filters documents from `index2` and of type `type` with the query 
`{ "terms" : { "tag" : [ "aaa" ] } }`. It then retrieves the ids of the documents from the field `id`
 specified by the parameter `path`. The list of ids is then used as filter and applied on the field 
 `foreign_key` of the documents from `index1`.

```json
    {
      "bool" : {
        "filter" : {
          "filterjoin" : {
            "foreign_key" : {
              "indices" : ["index2"],
              "types" : ["type"],
              "path" : "id",
              "query" : {
                "terms" : {
                  "tag" : [ "aaa" ]
                }
              }
            }
          }
        }
      }
    }
```

### Response Format

The response returned by the coordinate search API is identical to the response
returned by Elasticsearch's search API, but augmented with additional information
about the execution of the relational query planning. This additional information
is stored within the field named `coordinate_search` at the root of the response,
see example below. The object contains the following parameters:

* `actions`: a list of actions that has been executed - an action represents the execution of one single join.
* `relations`: the definition of the relations of the join - it contains two nested objects, `from` and `to`, one for each relation.
* `size`: the size of the filter used to compute the join, i.e., the number of terms across all shards used by the filterjoin.
* `size_in_bytes`: the size in bytes of the filter used to compute the join.
* `is_pruned`: a flag to indicate if the join computation has been pruned based on the `maxTermsPerShard` limit.
* `cache_hit`: a flag to indicate if the join was already computed and cached.
* `terms_encoding`: the terms encoding used to transfer terms across the network.
* `took`: the time it took to construct the filter.

```json
    {
      "coordinate_search": {
        "actions": [
          {
            "relations": {
              "from": {
                "indices": ["index2"],
                "types": ["type"],
                "field": "id"
              },
              "to": {
                "indices": null,
                "types": null,
                "field": "foreign_key"
              }
            },
            "size": 2,
            "size_in_bytes": 20,
            "is_pruned": false,
            "cache_hit": false,
            "terms_encoding" : "long",
            "took": 313
          }
        ]
      },
    ...
    }
```

## Performance Considerations

* We recommend to activate caching for all queries via the setting `index.queries.cache.everything: true`. The new
caching policy of Elasticsearch will not cache a `filterjoin` query on small segments which can lead to a significant
drop of performance. See issue [16529](https://github.com/elastic/elasticsearch/issues/16259) for more information.
* Joining numeric attributes is more efficient than joining string attributes.
* The bloom filter is the most efficient and the default encoding method for terms. It can encode 40M unique values
in ~30MB. However, this trades precision for space, i.e., the bloom filter can lead to false-positive results.
If precision is critical, then it is recommended to switch to the terms encoding to long.
* If the joined attributes of your documents contain incremental integers, switch the terms encoding to integer.
* The `filterjoin` includes a circuit breaker to prevent OOME when joining a field with a large number of unique values.
As a rule of thumb, the maximum amount of unique values transferred across the shards should be around 50 to 100M when
using bloom encoding, 5 to 10M when using long or integer encoding.
It is recommended to configure a `maxTermsPerShard` limit if the attribute defined by the `path` parameter contains
a larger number of values.
* The `bytes` terms encoding will likely provide better performance for highly selective queries over large indices, as
it will perform the filtering based on a dictionary lookup instead of a doc value scan.

## Acknowledgement

Part of this plugin is inspired and based on the pull request
[3278](https://github.com/elastic/elasticsearch/pull/3278) submitted by [Matt Weber](https://github.com/mattweber)
to the [Elasticsearch](https://github.com/elastic/elasticsearch) project.

- - -

Copyright (c) 2016, SIREn Solutions. All Rights Reserved.
