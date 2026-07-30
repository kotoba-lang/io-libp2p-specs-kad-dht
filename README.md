# io-libp2p-specs-kad-dht

[![CI](https://github.com/kotoba-lang/io-libp2p-specs-kad-dht/actions/workflows/ci.yml/badge.svg)](https://github.com/kotoba-lang/io-libp2p-specs-kad-dht/actions/workflows/ci.yml)

**libp2p Kademlia DHT** — the keyspace, the `/ipfs/kad/1.0.0` wire messages, the
iterative lookup as a pure state machine, and an **HTTP delegated-routing**
client that reads and writes the real DHT today. Portable `.cljc`; no crypto, no
sockets, no clock.

Built so [`ipns.record`](https://github.com/kotoba-lang/tech-ipfs-specs-ipns)
records can stop resolving through one proprietary registry.

## What this is, and what it is not — read this first

| | status |
|---|---|
| IPNS records that any IPFS implementation validates | **yes** — `tech-ipfs-specs-ipns`'s `ipns.record` |
| records published to and read from the real DHT | **yes**, via `kad.routing` (HTTP delegated routing) |
| resolution that does not depend on one server | **yes** — `kad.routing/resolve` requires a quorum across independently-operated routers, and validates every record itself |
| **this process being a DHT node** | **no** — see below |

A delegated router is an HTTP endpoint that *is* a DHT node and answers on your
behalf. A record published through it is the same signed record, at the same DHT
key, that any Kubo node would fetch, and any peer can resolve it. But you do not
hold a routing table, you answer nobody's queries, and you see the network only
through the endpoints you ask.

That is a real improvement over a single proprietary registry — the records
become interoperable and the routers are replaceable, plural, and run by
different people — and it is **not** the same thing as joining the DHT. Claiming
otherwise is the kind of thing this workspace exists to avoid.

**What full participation still needs**, precisely: multiaddr parsing, a TCP or
QUIC transport, a security handshake (Noise XX — [`kotoba-lang/noise`](https://github.com/kotoba-lang/noise)
already implements the pattern), multistream-select 1.0.0, a stream muxer
(Yamux), the identify protocol, and a k-bucket routing table. The **protocol
half is already here**: `kad.message` speaks `/ipfs/kad/1.0.0` and `kad.lookup`
is the algorithm. The gap is transport, and it is a project rather than a
patch.

## Modules

| ns | role |
|---|---|
| `kad.key` | record key → DHT key (`SHA-256`), XOR distance, closeness ordering, bucket index |
| `kad.message` | the `/ipfs/kad/1.0.0` `Message` and the libp2p `Record` it carries |
| `kad.lookup` | the iterative α=3 lookup, as `state + responses → state + next-queries` |
| `kad.routing` | HTTP delegated routing (`/routing/v1`), multi-router with quorum |

## Details that fail silently

**There are two byte strings and conflating them is the classic bug.** The
*record key* is `/ipns/<binary multihash>`; the *DHT key* is `SHA-256` of that.
Distance is measured on the second. A node measuring on raw record keys computes
a different topology from the whole network — not an error, just lookups that
return the wrong peers and records stored where nobody queries.

**Hashing is mandatory, not an optimization.** Every IPNS record key shares the
six-octet `/ipns/` prefix — 54 bits of agreement between two arbitrary names, as
the test suite measures. Unhashed, every IPNS name piles into one corner of the
keyspace and Kademlia's guarantees evaporate.

**Progress is measured against the closest peer, not the shortlist.** A round
returning fifty new peers, none closer than the current best, is *no progress* —
that is the termination condition. Measuring "did the shortlist grow" never
terminates on a large network.

**Failed peers are remembered as failed.** Otherwise the closest unreachable peer
is selected every round forever: never marked queried, so always eligible, and
always closest.

**A response answering a different key is detectable, and must be checked.** A
peer may return whatever it likes. Without `key-matches?`, a lookup for one name
can be answered with a valid, correctly-signed record for a *different* name —
every signature check passes and the resolver returns the wrong value. A
signature proves who wrote a record, never which question it answered.

**Quorum counts *validated* records.** `kad.routing/resolve` takes a
`validate-fn` and this library holds no crypto, so it cannot be the thing that
decides a record is genuine. Counting unvalidated records would let any router
win a name by answering first.

**`:not-found` and `:all-routers-failed` are different answers.** The obvious
spelling — "was every failed response a 404?" — returns true when there were no
failures at all, reporting "nobody has this name" for a case where every router
answered and nothing validated. There is a test.

## Usage

```clojure
(require '[kad.routing :as routing] '[ipns.record :as rec])

(routing/resolve http-fn "k51qzi5uqu5d…"
  {:routers ["https://my-kubo.internal/routing/v1"     ; run your own
             "https://delegated-ipfs.dev/routing/v1"]
   :quorum 2
   :validate-fn (fn [octets]
                  (let [r (rec/parse octets)
                        v (rec/validate r name {:verify-fn ed25519-verify
                                                :now-ms (now)})]
                    (when (:valid? v) (:fields v))))
   :select-fn rec/select})
;; => {:ok? true :record {…} :agreed 2 :routers ["https://my-kubo…" …]}
```

`http-fn` is `(fn [{:keys [method url headers body]}] -> {:status :headers :body})`.
No HTTP client is imported: the caller owns the network, the timeouts and the
TLS.

## Test

```
clojure -M:test
```

28 tests / 75 assertions, including a lookup converging over a simulated
200-peer network and one that terminates against a wholly unreachable one.
