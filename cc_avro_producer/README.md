

## Observations
Basic producer using confluent libraries.
This producer code follows all the recommended producer best practices.

```
   
    'client.id': 'my-customer-demo-app',
    'batch.size' : '55000',
    'linger.ms' : '50'
    'compression.type' : 'zstd',
    

```

During our tests we found that the request count dropped dramatically when batch size was increased to 50K and linger.ms was used to 50ms.
