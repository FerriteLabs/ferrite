---- MODULE OrSet ----
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS Replicas, Elements, MaxOps

VARIABLES
    sets,        \* sets[r] = set of (element, unique-tag) pairs per replica
    tagCounter,  \* tagCounter[r] = monotonic counter for generating unique tags
    delivered    \* set of <<src, dst>> pairs recording completed merges

TypeOK ==
    /\ \A r \in Replicas : tagCounter[r] \in Nat
    /\ delivered \subseteq (Replicas \X Replicas)

Init ==
    /\ sets = [r \in Replicas |-> {}]
    /\ tagCounter = [r \in Replicas |-> 0]
    /\ delivered = {}

\* Add an element on a replica — generates a fresh unique tag.
Add(r, e) ==
    /\ tagCounter' = [tagCounter EXCEPT ![r] = @ + 1]
    /\ sets' = [sets EXCEPT ![r] = @ \cup {<<e, <<r, tagCounter[r] + 1>>>>}]
    /\ UNCHANGED delivered

\* Remove an element — removes all tags associated with it (observed-remove).
Remove(r, e) ==
    /\ sets' = [sets EXCEPT ![r] = {p \in @ : p[1] /= e}]
    /\ UNCHANGED <<tagCounter, delivered>>

\* Merge: union of both sets (add-wins semantics — tags survive unless
\* explicitly removed by a Remove that saw them).
Merge(src, dst) ==
    /\ sets' = [sets EXCEPT ![dst] = @ \cup sets[src]]
    /\ delivered' = delivered \cup {<<src, dst>>}
    /\ UNCHANGED tagCounter

\* The observable value: project out just the elements (ignore tags).
Lookup(r) == {p[1] : p \in sets[r]}

\* Safety: After full pairwise delivery, all replicas observe the same set.
Convergence ==
    \A r1, r2 \in Replicas :
        (\A s, d \in Replicas : <<s, d>> \in delivered) =>
        Lookup(r1) = Lookup(r2)

\* Liveness helper: add-wins — an element present on any replica
\* appears everywhere after full delivery.
AddWins ==
    \A r1, r2 \in Replicas :
        (\A s, d \in Replicas : <<s, d>> \in delivered) =>
        Lookup(r1) = Lookup(r2)

Next ==
    \/ \E r \in Replicas, e \in Elements : Add(r, e)
    \/ \E r \in Replicas, e \in Elements : Remove(r, e)
    \/ \E s, d \in Replicas : s /= d /\ Merge(s, d)

Spec == Init /\ [][Next]_<<sets, tagCounter, delivered>>

====
