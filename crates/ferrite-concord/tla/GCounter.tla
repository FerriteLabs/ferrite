---- MODULE GCounter ----
EXTENDS Naturals, FiniteSets

CONSTANTS Replicas, MaxOps

VARIABLES counters, delivered

Init ==
    /\ counters = [r \in Replicas |-> [s \in Replicas |-> 0]]
    /\ delivered = {}

Increment(r) ==
    /\ counters' = [counters EXCEPT ![r][r] = @ + 1]
    /\ UNCHANGED delivered

Merge(src, dst) ==
    /\ counters' = [counters EXCEPT ![dst] =
        [s \in Replicas |-> IF counters[src][s] > counters[dst][s]
                           THEN counters[src][s]
                           ELSE counters[dst][s]]]
    /\ delivered' = delivered \cup {<<src, dst>>}

Value(r) ==
    LET sum[S \in SUBSET Replicas] ==
        IF S = {} THEN 0
        ELSE LET x == CHOOSE x \in S : TRUE
             IN counters[r][x] + sum[S \ {x}]
    IN sum[Replicas]

\* Safety: All replicas eventually converge to the same value.
Convergence ==
    \A r1, r2 \in Replicas :
        (\A s, d \in Replicas : <<s, d>> \in delivered) =>
        Value(r1) = Value(r2)

Next ==
    \/ \E r \in Replicas : Increment(r)
    \/ \E s, d \in Replicas : s /= d /\ Merge(s, d)

Spec == Init /\ [][Next]_<<counters, delivered>>

====
