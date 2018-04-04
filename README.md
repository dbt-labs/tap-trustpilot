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

This tap does _not_ support incremental replication!

---

Copyright &copy; 2018 Fishtown Analytics
