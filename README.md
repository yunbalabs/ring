# A consistent hashing ring

The code is based on [Riak Core](https://github.com/basho/riak_core).

## Usage

```erlang
1> Ring = ring:create(128, first_node).
2> ring:locate_key(ring:to_bin(Ring), <<"123">>).
{296867520082839655260123481645494988367611297792,26,
 first_node}
3> Ring2 = ring:add_node(Ring, second_node).
4> ring:locate_key(ring:to_bin(Ring2), <<"123">>).
{296867520082839655260123481645494988367611297792,26,
 second_node}
```