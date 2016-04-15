# This is Parallelogram.

Parallelogram is a parallelization library that distributes computationally intensive programs across machines in a distributed system using distribution mechanism similar to the ride-sharing service Uber.

## How exactly does distribution work?

Uber has a pool of users that can be classified as
either passengers or drivers. When a passenger Sarah requires a lift, she broadcasts a request, which alerts nearby drivers who can then decide to either accept or ignore the request. Drivers who are already carrying passengers oftentimes do not accept the request, and only those drivers who are completely available (and ideally nearby) consider picking up Sarah. Once an Uber driver confirms Sarah’s request, all other available drivers are notified that Sarah no longer needs a ride.

This ride­sharing system will serve as the foundation of our parallelization model. Our library will, on a single machine, partition pieces of the client’s program into parallelizable
chunks.

## What methods does this library expose?
* `map()`
* `filter()`
* `reduce()`
