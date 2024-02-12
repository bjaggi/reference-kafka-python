


At the very least, we recommend the producer to begin producing exclusively to the DR cluster in the event of a disaster (per manual intervention after some alert) instead of only writing to the DR cluster as part of handling the onError callback. We also recommend consumers begin reading from the DR cluster in the event of a disaster to support RTO requirements. To ensure all of the data is copied to the active cluster so it can act as the source of truth, we recommend using cluster linking to replicate any data written to the DR cluster back to the active cluster once the DR event has been resolved so the active cluster will have all data in two separate topics to support reprocessing requirements



