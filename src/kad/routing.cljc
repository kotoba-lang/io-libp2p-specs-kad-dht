(ns kad.routing
  "**HTTP delegated routing** (specs.ipfs.tech/routing/http-routing-v1) — how
  something that is not a libp2p node reads and writes the real DHT.

  ## Read this before believing anything about decentralization

  A delegated router is an HTTP endpoint that *is* a DHT node and answers on
  your behalf. Using one means:

  - **the records are real.** A record published through `/routing/v1/ipns/…`
    is the same signed IPNS record, at the same DHT key, that any Kubo node
    would fetch. It is stored in the actual DHT and any peer can resolve it.
  - **you are not a DHT node.** You do not hold a routing table, you answer
    nobody's queries, and you see the network only through whichever endpoints
    you ask.

  So this is a genuine improvement over a single proprietary registry — the
  records become interoperable and the routers are replaceable, plural, and run
  by different people — and it is **not** the same thing as joining the DHT.
  Saying otherwise would be the kind of claim this workspace exists to avoid.
  What full participation still needs is named in the README: multiaddr, a TCP
  or QUIC transport, multistream-select, a stream muxer, and the
  `/ipfs/kad/1.0.0` RPC over it. `kad.message` and `kad.lookup` are already the
  protocol half of that; the transport is the gap.

  ## Why more than one router

  A single endpoint is a single point of failure and a single point of lying.
  `resolve` queries several concurrently-supplied routers and requires
  `quorum` agreeing valid records before it answers. One router that returns a
  stale-but-validly-signed record cannot win against two that return the
  current one, because `ipns.record/select` prefers the higher sequence number
  and the records are *validated by the caller* before they are counted.

  That is the property that makes this meaningfully different from asking one
  server: no single router is trusted, and the trust that remains is
  cryptographic rather than positional.

  ## Transport is injected

  `http-fn` is `(fn [{:keys [method url headers body]}] -> {:status :headers :body})`
  with `body` as octets. No HTTP client is imported, for the same reason
  `godaddy-dns` injects `IDns`: the caller owns the network, the timeouts and
  the TLS, and a library that owned them would be untestable without one."
  (:refer-clojure :exclude [resolve])
  (:require [clojure.string :as str]))

(def default-routers
  "Public delegated routing endpoints. Listed as a *default*, not a
  recommendation: an operator serious about not depending on anyone else runs
  their own (Kubo exposes `/routing/v1` directly) and passes it here. These
  exist so the first call works rather than requiring infrastructure before the
  first experiment."
  ["https://delegated-ipfs.dev/routing/v1"])

(def ^:const ipns-record-content-type
  "The media type an IPNS record is fetched and published as. Asking for
  `application/json` from the same path returns a *different* thing — a JSON
  summary — and a client that does not set this header gets something it cannot
  verify a signature over."
  "application/vnd.ipfs.ipns-record")

(defn- ->octets [b]
  (cond
    (nil? b) nil
    (vector? b) b
    (string? b) (mapv #(bit-and (int %) 0xFF) (seq b))
    :else (mapv #(bit-and % 0xFF) (vec (seq #?(:clj b :cljs (array-seq b)))))))

(defn get-ipns
  "Fetch the raw IPNS record for `name` from one router.

  Returns `{:ok? true :record <octets> :router url}` or
  `{:ok? false :reason … :router url}`. Never throws: a resolver asking several
  routers must be able to score them all, and one refusing connections is an
  ordinary outcome rather than an exception."
  [http-fn router name]
  (try
    (let [{:keys [status body]} (http-fn {:method :get
                                          :url (str router "/ipns/" name)
                                          :headers {"Accept" ipns-record-content-type}})]
      (cond
        (= 404 status) {:ok? false :reason :not-found :router router}
        (not= 200 status) {:ok? false :reason :http-error :status status :router router}
        (nil? body) {:ok? false :reason :empty-body :router router}
        :else {:ok? true :record (->octets body) :router router}))
    (catch #?(:clj Exception :cljs :default) e
      {:ok? false :reason :transport-error :router router
       :detail #?(:clj (.getMessage e) :cljs (str e))})))

(defn put-ipns
  "Publish a serialized IPNS record for `name` through one router.

  The router validates the record before accepting it — which is the useful
  part: a 400 here means the record is malformed or badly signed, and finding
  that out at publish time is much better than discovering later that nothing
  resolves."
  [http-fn router name record-octets]
  (try
    (let [{:keys [status body]} (http-fn {:method :put
                                          :url (str router "/ipns/" name)
                                          :headers {"Content-Type" ipns-record-content-type}
                                          :body record-octets})]
      (if (<= 200 status 299)
        {:ok? true :router router :status status}
        {:ok? false :reason :rejected :status status :router router
         :detail (when body (apply str (map char (take 200 (->octets body)))))}))
    (catch #?(:clj Exception :cljs :default) e
      {:ok? false :reason :transport-error :router router
       :detail #?(:clj (.getMessage e) :cljs (str e))})))

;; ── multi-router resolution ───────────────────────────────────────────────

(defn score
  "The pure half of `resolve`: given per-router responses **already fetched**,
  validate each and pick the best.

  Split out because a synchronous `http-fn` is not universal. A JVM job can
  call one; a Cloudflare Worker cannot — `fetch` there is a Promise, and there
  is no way to await it inside a synchronous function. A Worker therefore does
  its own concurrent fetches, builds the same `{:ok? :record :router}` maps
  `get-ipns` produces, and calls this.

  Keeping the scoring pure also means the interesting logic — quorum,
  validation, selection, and telling `:not-found` from `:all-routers-failed` —
  is testable with no transport at all."
  [responses {:keys [quorum validate-fn select-fn] :or {quorum 1}}]
  (let [validated (keep (fn [r]
                          (when (:ok? r)
                            (when-let [v (validate-fn (:record r))]
                              (assoc r :validated v))))
                        responses)]
    (if (>= (count validated) quorum)
      {:ok? true
       :record (if select-fn
                 (select-fn (mapv :validated validated))
                 (:validated (first validated)))
       :agreed (count validated)
       :routers (mapv :router validated)
       :responses responses}
      {:ok? false
       ;; Order matters and the obvious spelling is wrong: asking whether every
       ;; *failed* response was :not-found returns true when there were no
       ;; failures at all, so a case where every router answered and nothing
       ;; validated would be reported as "nobody has this name". The three
       ;; outcomes a caller needs to tell apart are: nobody has it, nobody
       ;; answered, and the answers were not usable.
       :reason (let [ok (filter :ok? responses)]
                 (cond
                   (and (empty? ok) (seq responses)
                        (every? #(= :not-found (:reason %)) responses)) :not-found
                   (empty? ok) :all-routers-failed
                   :else :no-valid-record))
       :agreed (count validated)
       :quorum quorum
       :responses responses})))

(defn resolve
  "Resolve `name` through several routers and return the best **validated**
  record.

  Requires a **synchronous** `http-fn`, so it suits a JVM job and not a
  Cloudflare Worker; see `score` for the pure half a Worker composes with its
  own async fetches.

  `validate-fn` is `(fn [record-octets] -> validated-record-or-nil)` — the
  caller wires it to `ipns.record/parse` + `ipns.record/validate` with its own
  clock and verifier. This namespace deliberately cannot validate anything
  itself: it holds no crypto, so it cannot be the place that decides a record
  is genuine, and a router's answer is never trusted on the strength of having
  answered.

  `select-fn` picks among validated records — `ipns.record/select`, which
  prefers the higher sequence number.

  Returns `{:ok? true :record … :agreed n :routers [...]}` when at least
  `quorum` routers returned a record that validated, else `{:ok? false …}` with
  every router's outcome, so a caller can tell \"nobody has this name\" from
  \"every router was down\"."
  [http-fn name {:keys [routers quorum validate-fn select-fn]
                 :or {routers default-routers quorum 1}}]
  (score (mapv #(get-ipns http-fn % name) routers)
         {:quorum quorum :validate-fn validate-fn :select-fn select-fn}))

(defn publish
  "Publish through every router, and report per-router outcomes.

  Succeeds if **any** router accepted: the record is in the DHT once one node
  has it, and the others will learn it by ordinary replication. Requiring all
  of them would make one unreachable endpoint a publishing failure when the
  publish actually worked."
  [http-fn name record-octets {:keys [routers] :or {routers default-routers}}]
  (let [results (mapv #(put-ipns http-fn % name record-octets) routers)
        ok (filterv :ok? results)]
    {:ok? (boolean (seq ok))
     :accepted (mapv :router ok)
     :rejected (filterv (complement :ok?) results)}))

(defn router-summary
  "One line per router — for an operator who needs to know *which* endpoint is
  answering, not just that resolution worked."
  [{:keys [responses]}]
  (str/join "\n"
            (for [r responses]
              (str (:router r) " → "
                   (if (:ok? r)
                     (str "ok, " (count (:record r)) " octets")
                     (str (name (:reason r))
                          (when (:status r) (str " " (:status r)))))))))
