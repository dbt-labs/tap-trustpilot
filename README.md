# tap-trustpilot

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [TrustPilot](https://developers.trustpilot.com/)
- Extracts the following resources:
  - [Business Unit Reviews](https://developers.trustpilot.com/business-units-api#get-a-business-unit's-reviews)
  - [Consumers](https://developers.trustpilot.com/consumer-api#get-the-profile-of-the-consumer(with-#reviews-and-weblinks))
- Outputs the schema for each resource

This tap does support incremental replication for private reviews, see below.


### Configuration

Create a `config.json` file that looks like this:

```
{
    "access_key": "...",
    "client_secret": "...",
    "username": "user@email.com",
    "password": "hunter2",
    "business_units": ["my_domain.com", "www.my_domain.co.uk"],
    "user_agent": "tap-trustpilot <my.email@domain.com>",
    "start_date": "2000-01-01T00:00:00Z"
}
```

| Config property    | Required  | Description
| ------------------ | --------- | --------------
| `access_key`       | Yes       | See https://documentation-apidocumentation.trustpilot.com/authentication
| `client_secret`    | For auth. | See https://documentation-apidocumentation.trustpilot.com/authentication
| `username`         | For auth. | Username of your TrustPilot account
| `password`         | For auth. | Username of your TrustPilot account
| `business_units`   | Alternate to `business_unit_id` | An array of business units by their name. The tap will request the business unit ID.<br/>When this config property is used `business_unit_id` is ignored.
| `business_unit_id` | Yes       | A single business unit ID from TrustPilot
| `user_agent`       | No        | User agent to be used for HTTP requests
| `start_date`       | No        | For streams with replication method `INCREMENTAL` the start date time to be used

When required is 'For auth', it is only required when the stream requires an authentication.

### Supported streams

| stream name       | Requires auth. | Replication method | Content
| ----------------- | -------------- | ------------------ | ---------------------
| `business_units`  | No             | FULL_TABLE         | Business unit profile info data
| `reviews`         | No             | FULL_TABLE         | Public business unit reviews
| `consumers`       | No             | FULL_TABLE         | Consumer information from public reviews. Requires stream `reviews`
| `private_reviews` | Yes            | INCREMENTAL        | Private business unit reviews

---

Copyright &copy; 2018 Fishtown Analytics<br/>
Copyright &copy; 2020 Horze International GmbH
