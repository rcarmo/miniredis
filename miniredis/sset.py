from bisect import bisect_left, bisect_right
from collections.abc import MutableSet # Use ABC for abstract base class


class SortedSet(MutableSet):
    """
    Redis-style SortedSet implementation using bisect.

    Maintains two internal data structures:
    1. A sorted list of (score, member) pairs.
    2. A dictionary from member to score.

    Note: Insertion and removal are O(N) due to list insertion/deletion.
    For better performance (O(log N)), consider alternative structures
    like balanced trees or skip lists if N becomes large.
    """
    def __init__(self, iterable=None):
        """
        Create a sorted set. Optional iterable can initialize the set.
        Iterable should yield (member, score) pairs.
        """
        # sorted list of (score, member)
        self._scores = []
        # dictionary from member to score
        self._members = {}
        if iterable is not None:
            for member, score in iterable:
                self.add(member, score)

    # Required MutableSet abstract methods
    def add(self, member, score=0.0):
        """
        Add member with score. If member is already present,
        update its score. Conforms to Set.add signature partially,
        but requires score.
        Returns True if member was added, False if updated.
        """
        try:
            # Convert score to float for consistent comparison
            score = float(score)
        except (ValueError, TypeError):
            raise ValueError("Score must be a float or representable as a float")

        found = member in self._members
        if found:
            self._remove_existing(member)

        # Find insertion point and insert
        index = bisect_left(self._scores, (score, member))
        self._scores.insert(index, (score, member))
        self._members[member] = score
        return not found

    def discard(self, member):
        """
        Remove member from the set if it is present.
        Returns True if member was removed, False otherwise.
        Conforms to Set.discard signature.
        """
        if member not in self._members:
            return False
        self._remove_existing(member)
        return True

    def __contains__(self, member):
        return member in self._members

    def __len__(self):
        return len(self._members)

    def __iter__(self):
        """Iterate over members in score order."""
        for _, member in self._scores:
            yield member

    # --- End Required MutableSet methods ---

    def _remove_existing(self, member):
        """Internal helper to remove an existing member."""
        score = self._members[member]
        # Find the exact (score, member) pair to remove
        # bisect_left gives the insertion point, which is the index if found
        score_index = bisect_left(self._scores, (score, member))
        # Verify we found the correct item before deleting
        if score_index < len(self._scores) and self._scores[score_index] == (score, member):
            del self._scores[score_index]
            del self._members[member]
        else:
            # This indicates an internal inconsistency, should not happen
            raise RuntimeError(f"Internal state inconsistency: could not find {member} with score {score} for removal")


    def clear(self):
        """
        Remove all members and scores from the sorted set.
        """
        self._scores = []
        self._members = {}

    def __str__(self):
        # Represent as a set of members for clarity, though order is lost
        return "{{{}}}".format(", ".join(repr(m) for m in self))

    def __repr__(self):
        # Show the internal structure for debugging
        return "SortedSet([{}])".format(", ".join(f"({s!r}, {m!r})" for s, m in self._scores))

    # Redis-specific methods
    def zadd(self, score, member, *args):
        """
        Adds members with scores. Handles multiple score-member pairs.
        Returns the number of elements added (not updated).
        """
        if len(args) % 2 != 0:
            raise ValueError("ZADD requires score-member pairs")

        added_count = 0
        pairs = [(score, member)] + list(zip(args[::2], args[1::2]))

        for s, m in pairs:
            if self.add(m, s):
                added_count += 1
        return added_count

    def zrem(self, *members):
        """
        Removes members from the sorted set.
        Returns the number of members removed.
        """
        removed_count = 0
        for member in members:
            if self.discard(member):
                removed_count += 1
        return removed_count

    def zscore(self, member):
        """
        Get the score for a member.
        Returns score (as float) or None if member not found.
        """
        return self._members.get(member)

    def zrank(self, member):
        """
        Get the rank (0-based index) of a member, ordered by score (ascending).
        Returns rank or None if member not found.
        """
        score = self._members.get(member)
        if score is None:
            return None
        # Find the first occurrence of this score
        index = bisect_left(self._scores, (score, member))
        # Verify it's the correct member
        if index < len(self._scores) and self._scores[index] == (score, member):
            return index
        else:
             # Should not happen if member is in _members
             raise RuntimeError(f"Internal state inconsistency: could not find rank for {member}")

    def zrevrank(self, member):
        """
        Get the rank (0-based index) of a member, ordered by score (descending).
        Returns rank or None if member not found.
        """
        rank = self.zrank(member)
        return (len(self) - 1 - rank) if rank is not None else None

    def _parse_range_args(self, start, end):
        """Helper to parse Redis-style range arguments."""
        try:
            start = int(start)
            end = int(end)
        except (ValueError, TypeError):
            raise ValueError("start and end must be integers")

        length = len(self)
        # Convert negative indices
        if start < 0:
            start = length + start
        if end < 0:
            end = length + end

        # Clamp indices to valid range [0, length-1]
        start = max(0, start)
        # end is inclusive, so clamp to length-1
        end = min(length - 1, end)

        return start, end, length

    def zrange(self, start, end, withscores=False, desc=False):
        """
        Return members (and optionally scores) in the specified range of ranks.
        start and end are 0-based indices, inclusive.
        Negative indices count from the end (-1 is the last element).
        """
        start, end, length = self._parse_range_args(start, end)

        if start > end or start >= length:
            return [] # Empty range

        # Python slice end index is exclusive, Redis is inclusive
        slice_end = end + 1

        if desc:
            # Calculate reversed indices for slicing
            rev_start = length - slice_end
            rev_end = length - start
            items = reversed(self._scores[rev_start:rev_end])
        else:
            items = self._scores[start:slice_end]

        if withscores:
            # Return list of [member, score_str]
            return [[m, str(s)] for s, m in items]
        else:
            # Return list of members
            return [m for s, m in items]

    def zrevrange(self, start, end, withscores=False):
        """
        Return members (and optionally scores) in the specified range of ranks,
        ordered from highest to lowest score.
        """
        return self.zrange(start, end, withscores=withscores, desc=True)

    def _parse_score_range_args(self, min_score, max_score):
        """Helper to parse score range arguments."""
        min_inclusive = True
        max_inclusive = True

        if isinstance(min_score, str) and min_score.startswith('('):
            min_inclusive = False
            min_score = min_score[1:]

        if isinstance(max_score, str) and max_score.startswith('('):
            max_inclusive = False
            max_score = max_score[1:]

        try:
            if min_score == '-inf':
                min_f = float('-inf')
            else:
                min_f = float(min_score)

            if max_score == '+inf':
                max_f = float('+inf')
            else:
                max_f = float(max_score)
        except (ValueError, TypeError):
            raise ValueError("min and max scores must be floats or representable as floats")

        return min_f, max_f, min_inclusive, max_inclusive

    def zrangebyscore(self, min_score, max_score, withscores=False, limit=None):
        """
        Return members (and optionally scores) with scores between min_score and max_score.
        min/max can be exclusive using '(' prefix (e.g., '(1.0').
        '-inf' and '+inf' are valid.
        limit is an optional (offset, count) tuple.
        """
        if not self:
            return []

        min_f, max_f, min_incl, max_incl = self._parse_score_range_args(min_score, max_score)

        # Find the start index
        if min_incl:
            # Find first element >= min_f
            left = bisect_left(self._scores, (min_f,))
        else:
            # Find first element > min_f
            left = bisect_right(self._scores, (min_f, float('inf'))) # Find insertion point for score > min_f

        # Find the end index
        if max_incl:
            # Find first element > max_f
            right = bisect_right(self._scores, (max_f, float('inf')))
        else:
            # Find first element >= max_f
            right = bisect_left(self._scores, (max_f,))

        # Slice the relevant portion
        items = self._scores[left:right]

        # Apply limit if provided
        if limit is not None:
            try:
                offset, count = map(int, limit)
                if offset < 0 or count <= 0:
                     # Redis returns empty list for invalid limit
                     # although some versions might error.
                     # Let's return empty list for simplicity.
                     items = [] 
                else:
                    items = items[offset : offset + count]
            except (ValueError, TypeError, IndexError):
                raise ValueError("limit requires two integer arguments: offset, count")

        if withscores:
            return [[m, str(s)] for s, m in items]
        else:
            return [m for s, m in items]

    def zrevrangebyscore(self, max_score, min_score, withscores=False, limit=None):
        """
        Return members (and optionally scores) with scores between max_score and min_score,
        ordered from highest to lowest score.
        """
        # Get the range in ascending order first
        ascending_items = self.zrangebyscore(min_score, max_score, withscores=True, limit=None)

        # Reverse the result
        items = list(reversed(ascending_items))

        # Apply limit after reversing
        if limit is not None:
            try:
                offset, count = map(int, limit)
                if offset < 0 or count <= 0:
                    items = []
                else:
                    items = items[offset : offset + count]
            except (ValueError, TypeError, IndexError):
                raise ValueError("limit requires two integer arguments: offset, count")

        # Format output based on withscores
        if withscores:
            return items # Already in [member, score_str] format
        else:
            return [m for m, s in items]

    def zcard(self):
        """
        Return the number of elements in the sorted set.
        """
        return len(self)

    def zcount(self, min_score, max_score):
        """
        Return the number of elements with scores between min_score and max_score.
        """
        # Use zrangebyscore logic to find the range and return its length
        if not self:
            return 0

        min_f, max_f, min_incl, max_incl = self._parse_score_range_args(min_score, max_score)

        if min_incl:
            left = bisect_left(self._scores, (min_f,))
        else:
            left = bisect_right(self._scores, (min_f, float('inf')))

        if max_incl:
            right = bisect_right(self._scores, (max_f, float('inf')))
        else:
            right = bisect_left(self._scores, (max_f,))

        return max(0, right - left)

    # --- Methods below are less common or might need refinement ---

    def score(self, member):
        """
        Alias for zscore, potentially deprecated.
        Get the score for a member.
        """
        return self.zscore(member)

    def rank(self, member):
        """
        Alias for zrank, potentially deprecated.
        Get the rank (index of a member).
        """
        return self.zrank(member)

    def range(self, start, end, desc=False, withscores=False):
        """
        Alias for zrange/zrevrange, potentially deprecated.
        Return members/scores between min and max ranks.
        """
        return self.zrange(start, end, desc=desc, withscores=withscores)

    def scorerange(self, start, end, withscores=False):
        """
        Alias for zrangebyscore, potentially deprecated.
        Return members/scores between min and max scores.
        """
        return self.zrangebyscore(start, end, withscores=withscores)

    def items(self):
        """Return an iterator over (score, member) pairs."""
        return iter(self._scores)

    def min_score(self):
        """Return the minimum score in the set."""
        if not self: raise IndexError("SortedSet is empty")
        return self._scores[0][0]

    def max_score(self):
        """Return the maximum score in the set."""
        if not self: raise IndexError("SortedSet is empty")
        return self._scores[-1][0]

# Example Usage:
if __name__ == '__main__':
    zs = SortedSet()
    zs.zadd(1, 'one')
    zs.zadd(3, 'three')
    zs.zadd(2, 'two')
    zs.zadd(2, 'deux') # Add another member with score 2

    print(f"Set: {zs!r}")
    print(f"Members (iter): {list(zs)}")
    print(f"Length: {len(zs)}")
    print(f"Contains 'two': {'two' in zs}")
    print(f"Score of 'three': {zs.zscore('three')}")
    print(f"Rank of 'two': {zs.zrank('two')}") # Rank depends on member name for ties
    print(f"Rank of 'deux': {zs.zrank('deux')}")
    print(f"RevRank of 'one': {zs.zrevrank('one')}")

    print(f"Range 0-2: {zs.zrange(0, 2)}")
    print(f"Range 0-2 with scores: {zs.zrange(0, 2, withscores=True)}")
    print(f"RevRange 0-1: {zs.zrevrange(0, 1)}")

    print(f"Range by score 1.5-2.5: {zs.zrangebyscore(1.5, 2.5)}")
    print(f"Range by score (1-3: {zs.zrangebyscore('(1', '3')}")
    print(f"Range by score 1-3 with scores: {zs.zrangebyscore(1, 3, withscores=True)}")
    print(f"Count score 1-2: {zs.zcount(1, 2)}")

    zs.discard('two')
    print(f"After discarding 'two': {zs!r}")
    print(f"Length: {len(zs)}")


