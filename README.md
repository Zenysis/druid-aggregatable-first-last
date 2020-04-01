## Druid Aggregatable First/Last Aggregator
The aggregatable first/last aggregator wraps any given aggregator but only
aggregates the values when the row's timestamp is either the largest or smallest
timestamp seen in the rows of a [Druid](http://druid.io/) query. This extension
is an improvement over the native [First/Last aggregators](https://druid.apache.org/docs/latest/querying/aggregations.html#first-last-aggregator).

In the native first/last aggregation, only a single value is tracked by the
aggregator. This is a frustrating limitation, since there are many use cases
where a single value is not useful and an aggregated value is preferred.

The aggregatable first/last aggregator is similar to the [Filtered Aggregator](https://druid.apache.org/docs/latest/querying/aggregations.html#filtered-aggregator). You specify a nested aggregator
that should be used when multiple rows have the same timestamp.

### Installation
You can either use the prebuilt jar located in `dist/` or build from source
using the directions below.

Once you have the compiled `jar`, copy it to your druid installation and
follow the
[including extension](http://druid.io/docs/latest/operations/including-extensions.html)
druid documentation. You should end up adding a line similar to this to your
`common.runtime.properties` file:

`druid.extensions.loadList=["druid-aggregatable-first-last"]`

##### Building from source
Clone the druid repo and this line to `pom.xml` in the "Community extensions"
section:

```xml
<module>${druid-aggregatable-first-last}/aggregatable-first-last</module>
```
replacing `${druid-aggregatable-first-last}` with your path to this
repo.

Then, inside the druid repo, run:
`mvn package -DskipTests=true -rf :druid-aggregatable-first-last`
This will build the aggregator extension and place it in
`${druid-aggregatable-first-last}/aggregatable-first-last/target`


### Use
To use the aggregatable first/last aggregator, specify the aggregation type and
the aggregator to use:
```javascript
{
  "type": "aggregateFirst",
  "aggregator": <aggregation>
}
...
{
  "type": "aggregateLast",
  "aggregator": <aggregation>
}
```

During querying, every row will pass through the first/last aggregator. In the
`aggregateFirst` case, if the current row has the minimum timestamp seen, the
row will be passed along to the nested aggregator for calculation. If a new
row comes in that has a smaller timestamp, the nested aggregator is reset and
calculation will restart with the new row. In the `aggregateLast` case, the
same process occurs only using the maximum timestamp.

### Example

##### Total registrations by week
Often, a user will want to store cumulative event data inside Druid. By storing
cumulative data, a user would not need to query the entire range of data inside
Druid to calculate the total value. They could just use the latest cumulative
value instead.

In this scenario, the datasource being queried stores the
"total cumulative number of user registrations" each day. To calculate the total
number of user registrations by week, you could use the `aggregateLast`
aggregator paired with a `longSum` aggregator.
```javascript
{
  ...
  "granularity": "week",
  "aggregations": [
    {
      "type": "aggregateLast",
      "aggregator": {
        "type": "longSum",
        "fieldName": "total_registrations_value",
        "name": "total_registrations"
      }
    }
  ],
  ...
}
```
The query can be easily changed to group by Month without needing to change the
aggregator. Also, if the cumulative events are stored at a low dimension (say
at the Store level in the dimension hierarchy Region -> District -> Store) the
aggregator will stay the same even when breaking down the total registrations at
different levels of the dimension hierarchy (like District).

#### Future Features
It seems useful to be able to specify a time format alongside the query so that
users can choose the granularity at which the first/last timestamp is stored at.
One example would be a datasource where values are stored by hour, but you want
to take a sum of the last day's values. In the current implementation, the
full timestamp is used, so only the last hour's data would be passed to the
nested aggregator for calculation.
