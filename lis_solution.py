"""
Longest Increasing Subsequence (LIS)
=====================================

Given an array of n integers, find the length of the longest increasing
subsequence. A subsequence keeps the relative order of elements (it does not
need to be contiguous) and here "increasing" means *strictly* increasing:
a[i0] < a[i1] < ... < a[ik-1] with i0 < i1 < ... < ik-1.

This module contains three implementations:
  1. lis_n2        -- classic O(n^2) dynamic programming.
  2. lis_nlogn     -- O(n log n) dynamic programming via "tails" array.
  3. brute_force   -- exponential reference used only to verify the others.
"""

from bisect import bisect_left
from itertools import combinations
from random import randint


# ---------------------------------------------------------------------------
# 1. O(n^2) dynamic programming
# ---------------------------------------------------------------------------
def lis_n2(arr):
    """
    Returns the length of the longest strictly increasing subsequence.

    Subproblem / recurrence
    -----------------------
    Let dp[i] = length of the longest increasing subsequence that ENDS at
    index i (i.e. whose last element is arr[i]).

    Base case: dp[i] = 1 for every i, because the single-element subsequence
    [arr[i]] is always a valid increasing subsequence.

    Transition: to extend a subsequence that ends at some earlier index j
    with the element arr[i], we need arr[j] < arr[i].  Among all such j we
    pick the one giving the longest chain:

        dp[i] = 1 + max( dp[j]  for  j < i and arr[j] < arr[i] )

    If no such j exists, the max over an empty set is 0, so dp[i] stays 1.

    Answer: the overall LIS may end anywhere, so

        answer = max(dp[i] for i in range(n))

    This is a valid DP because dp[i] depends only on dp[j] with j < i, which
    are already computed in a left-to-right scan (optimal substructure + no
    cyclic dependencies).
    """
    n = len(arr)
    dp = [1] * n  # dp[i] = best length of a subsequence ending at i

    for i in range(n):
        # Look back at every earlier element that is strictly smaller.
        for j in range(i):
            if arr[j] < arr[i]:
                dp[i] = max(dp[i], dp[j] + 1)

    return max(dp, default=0)


# ---------------------------------------------------------------------------
# 2. O(n log n) dynamic programming (patience sorting / tails array)
# ---------------------------------------------------------------------------
def lis_nlogn(arr):
    """
    Returns the length of the longest strictly increasing subsequence in
    O(n log n) time and O(n) space.

    Key idea
    --------
    Maintain tails[k] = the smallest possible "last element" of any increasing
    subsequence of length (k+1) seen so far.  tails is always sorted.

    For each element x:
      * If x is greater than every tail, x can extend the longest subsequence
        we have, so we append it (a longer subsequence now exists).
      * Otherwise, binary search for the first tail >= x and replace it with
        x.  Replacing a tail with a smaller value never decreases the length
        of any achievable subsequence, and it makes future extensions easier
        (greedy: smaller tails are better).

    The invariant tails is sorted holds because each replacement preserves
    ordering:  tails[k-1] < x <= tails[k] by construction of the binary search.

    The length of tails at the end equals the LIS length.  (Note: tails is
    NOT itself the actual LIS, it only tracks lengths / optimal tails.)
    """
    tails = []  # tails[k] = minimal possible last value of a length-(k+1) LIS

    for x in arr:
        # First position in tails where tails[pos] >= x.
        pos = bisect_left(tails, x)
        if pos == len(tails):
            tails.append(x)   # x extends the longest subsequence found so far
        else:
            tails[pos] = x    # replace: a smaller tail of this length is better

    return len(tails)


# ---------------------------------------------------------------------------
# 3. Brute force (reference, O(2^n) -- for testing only)
# ---------------------------------------------------------------------------
def brute_force(arr):
    """Enumerates every subsequence and returns the longest increasing one."""
    n = len(arr)
    best = 0
    for size in range(1, n + 1):
        for sub in combinations(arr, size):
            if all(sub[i] < sub[i + 1] for i in range(size - 1)):
                best = size
                break  # once a valid subsequence of this size exists, it wins
    return best


# ---------------------------------------------------------------------------
# Self-test: compare all three implementations on random small arrays
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    failures = 0
    for trial in range(2000):
        n = randint(0, 9)
        arr = [randint(-5, 5) for _ in range(n)]

        expected = brute_force(arr)
        got_n2 = lis_n2(arr)
        got_nlogn = lis_nlogn(arr)

        if not (got_n2 == expected == got_nlogn):
            failures += 1
            print(f"MISMATCH on {arr}: brute={expected}, n2={got_n2}, nlogn={got_nlogn}")

    # A few hand-checked cases.
    assert lis_n2([]) == 0
    assert lis_n2([5, 5, 5]) == 1                      # strictly increasing only
    assert lis_n2([1, 2, 3]) == 3
    assert lis_n2([3, 2, 1]) == 1
    assert lis_n2([10, 9, 2, 5, 3, 7, 101, 18]) == 4   # [2, 3, 7, 101] or [2,5,7,101]
    assert lis_nlogn([10, 9, 2, 5, 3, 7, 101, 18]) == 4

    if failures:
        print(f"{failures} random trials FAILED")
    else:
        print("All 2000 random trials + hand-checked cases passed.")
