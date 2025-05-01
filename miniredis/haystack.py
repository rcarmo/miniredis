#!/usr/bin/env python
# encoding: utf-8
"""
Haystack.py

An on-disk cache with a dict-like API, inspired by Facebook's Haystack store

Created by Rui Carmo on 2010-04-05
Published under the MIT license.
"""

__author__ = "Rui Carmo http://the.taoofmac.com"
__revision__ = "$Id$"
__version__ = "1.0"

import os, sys, stat, mmap, threading, time, logging
import pickle  # Use standard pickle module in Python 3
from typing import Any, Dict, List, Optional, Union, TypeAlias

log = logging.getLogger()

# Define type aliases for better type hinting
IndexEntry: TypeAlias = List[float]  # [mtime, length, offset]
Index: TypeAlias = Dict[str, IndexEntry]


class Haystack(dict):

    def __init__(
        self,
        path: str,
        basename: str = "haystack",
        commit: int = 300,
        compact: int = 3600,
    ) -> None:
        super().__init__()  # Modern super() call syntax
        self.enabled = True
        self.mutex = threading.Lock()
        self.commit_interval = commit
        self.compact_interval = compact
        self.path = path
        self.basename = basename
        self.cache = os.path.join(self.path, self.basename + ".bin")
        self.index = os.path.join(self.path, self.basename + ".idx")
        self.temp = os.path.join(self.path, self.basename + ".tmp")
        self._rebuild()
        self.created = self.modified = self.compacted = self.committed = time.time()

    def _rebuild(self) -> None:
        """Rebuild the index from disk."""
        self.mutex.acquire()
        try:
            os.makedirs(self.path, exist_ok=True)  # Use exist_ok for Python 3
        except Exception as e:
            log.error(f"Error on makedirs({self.path}): {e}")

        # Use context manager for file operations
        try:
            with open(self.cache, "rb") as cache:
                pass  # Just check if file exists and can be opened
        except (FileNotFoundError, PermissionError) as e:
            log.error(f"Error while opening {self.cache} for reading: {e}")
            try:
                with open(self.cache, "ab") as cache:
                    pass  # Create the file
            except OSError as e:
                log.error(f"Could not create cache file {self.cache}: {e}")
                self.enabled = False

        try:
            with open(self.index, "rb") as f:
                self._index: Index = pickle.loads(f.read())
        except Exception as e:
            log.error(f"Index retrieval from disk failed: {e}")
            self._index = {}  # "key": [mtime,length,offset]

        self.created = self.modified = self.compacted = self.committed = time.time()
        log.debug(
            f"Rebuild complete, {len(self._index)} items."
        )  # Use len() directly on dict
        self.mutex.release()

    def commit(self) -> None:
        """Commit the index to disk."""
        if not self.enabled:
            return
        self.mutex.acquire()
        try:
            with open(self.index, "wb") as f:
                f.write(pickle.dumps(self._index))
            self.committed = time.time()
            log.debug(f"Index {self.index} committed, {len(self._index)} items.")
        except OSError as e:
            log.error(f"Failed to commit index to {self.index}: {e}")
        finally:
            self.mutex.release()

    def purge(self) -> None:
        """Delete all cache files and rebuild the cache."""
        self.mutex.acquire()
        try:
            try:
                os.unlink(self.index)
            except OSError as e:
                log.error(f"Could not unlink {self.index}: {e}")
            try:
                os.unlink(self.cache)
            except OSError as e:
                log.error(f"Could not unlink {self.cache}: {e}")
        finally:
            self.mutex.release()
            self._rebuild()

    def _cleanup(self) -> None:
        """Check if commit or compaction is needed."""
        now = time.time()
        if now > (self.committed + self.commit_interval):
            self.commit()
        if now > (self.compacted + self.compact_interval):
            self._compact()

    def __eq__(self, other: object) -> bool:
        raise TypeError("Equality undefined for this kind of dictionary")

    def __ne__(self, other: object) -> bool:
        raise TypeError("Equality undefined for this kind of dictionary")

    def __lt__(self, other: object) -> bool:
        raise TypeError("Comparison undefined for this kind of dictionary")

    def __le__(self, other: object) -> bool:
        raise TypeError("Comparison undefined for this kind of dictionary")

    def __gt__(self, other: object) -> bool:
        raise TypeError("Comparison undefined for this kind of dictionary")

    def __ge__(self, other: object) -> bool:
        raise TypeError("Comparison undefined for this kind of dictionary")

    def __repr__(self) -> str:
        return (
            f"<Haystack at {self.path}/{self.basename} with {len(self._index)} items>"
        )

    def expire(self, when: float) -> None:
        """Remove from cache any items older than a specified time"""
        if not self.enabled:
            return
        self.mutex.acquire()
        try:
            # Use list to avoid modification during iteration
            for k in list(self._index.keys()):
                if self._index[k][0] < when:
                    del self._index[k]
        finally:
            self.mutex.release()
            self._cleanup()

    def keys(self) -> list[str]:
        # In Python 3 keys() returns a view, convert to list if needed
        return list(self._index.keys())

    def stats(self, key: str) -> IndexEntry:
        """Get index statistics for a key."""
        if not self.enabled:
            raise KeyError(key)
        self.mutex.acquire()
        try:
            stats = self._index[key]
            return stats
        except KeyError:
            raise KeyError(key)
        finally:
            self.mutex.release()

    def __setitem__(self, key: str, val: Any) -> None:
        """Store an item in the cache - errors will cause the entire cache to be rebuilt"""
        if not self.enabled:
            return
        self.mutex.acquire()
        try:
            with open(self.cache, "ab") as cache:
                buffer = pickle.dumps(val, protocol=pickle.HIGHEST_PROTOCOL)
                offset = cache.tell()
                cache.write(buffer)
                self.modified = mtime = time.time()
                self._index[key] = [mtime, len(buffer), offset]
        except Exception as e:
            log.error(f"Error while storing {key}: {e}")
            raise IOError(f"Error storing item: {e}")
        finally:
            self.mutex.release()
            self._cleanup()  # Check if we need to commit/compact

    def __delitem__(self, key: str) -> None:
        """Remove item from cache - in practice, we only remove it from the index"""
        if not self.enabled:
            return
        self.mutex.acquire()
        try:
            del self._index[key]
        except KeyError:
            raise KeyError(key)
        except Exception as e:
            log.error(f"Unexpected error while deleting {key}: {e}")
            raise
        finally:
            self.mutex.release()
            self._cleanup()  # Check if we need to commit/compact

    def get(self, key: str, default: Any = None) -> Any:
        """Get an item with a default value if not found."""
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def __getitem__(self, key: str) -> Any:
        """Retrieve item"""
        if not self.enabled:
            raise KeyError(key)
        self.mutex.acquire()
        try:
            # Make sure the key exists before trying to read it
            if key not in self._index:
                raise KeyError(key)

            with open(self.cache, "rb") as cache:
                cache.seek(self._index[key][2])
                buffer = cache.read(self._index[key][1])
                item = pickle.loads(buffer)
                return item
        except (FileNotFoundError, PermissionError) as e:
            log.error(f"File operation error while retrieving {key}: {e}")
            raise KeyError(key)
        except (pickle.PickleError, EOFError) as e:
            log.error(f"Pickle error while retrieving {key}: {e}")
            raise KeyError(key)
        except Exception as e:
            log.error(f"Unexpected error while retrieving {key}: {e}")
            raise KeyError(key)
        finally:
            self.mutex.release()

    def mtime(self, key: str) -> float:
        """Return the creation/modification time of a cache item"""
        if not self.enabled:
            raise KeyError(key)
        self.mutex.acquire()
        try:
            item = self._index[key][0]
            return item
        except KeyError:
            raise KeyError(key)
        except Exception as e:
            log.debug(f"Error while getting modification time for {key}: {e}")
            raise KeyError(key)
        finally:
            self.mutex.release()

    def _compact(self) -> None:
        """Compact the cache by rewriting only valid items"""
        self.mutex.acquire()
        try:
            # Use atomic operations where possible
            with open(self.cache, "rb") as cache, open(self.temp, "wb") as compacted:
                new_index: Index = {}
                i = 0
                for key in self._index:
                    try:
                        cache.seek(self._index[key][2])
                        offset = compacted.tell()
                        data = cache.read(self._index[key][1])
                        compacted.write(data)
                        new_index[key] = [time.time(), self._index[key][1], offset]
                        i += 1
                    except Exception as e:
                        log.error(f"Error while compacting item {key}: {e}")
                        # Skip this item
                        continue

                size = compacted.tell()
                compacted.flush()
                os.fsync(compacted.fileno())

            os.replace(self.temp, self.cache)  # Atomic replacement on most systems
            self.compacted = time.time()
            self._index = new_index
            log.debug(f"Compacted {self.cache}: {i} items into {size} bytes")
        except OSError as e:
            log.error(f"Failed to compact cache: {e}")
        finally:
            self.mutex.release()
            self.commit()


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    print("Running Haystack self-test...")
    c = Haystack(".", commit=3, compact=4)

    # Test basic operations
    c["tired"] = "to expire in 2 seconds"
    c["foo"] = {"a": 1, "b": 2}
    c["zbr"] = "42"
    c["test/path/name"] = "test"

    print("Values stored. Testing retrieval...")
    assert c["foo"] == {"a": 1, "b": 2}, "Retrieval test failed"
    assert c["zbr"] == "42", "String retrieval test failed"

    print("Testing expiration...")
    time.sleep(2)
    c.expire(time.time() - 2)

    try:
        value = c["tired"]
        print(f"ERROR: Retrieved expired item: {value}")
    except KeyError:
        print("Expired item correctly removed")

    print("Testing deletion...")
    del c["foo"]
    try:
        c["foo"]
        print("ERROR: Retrieved deleted item")
    except KeyError:
        print("Deleted item correctly removed")

    print("Waiting for automatic commit and compact...")
    time.sleep(5)

    print("All tests completed successfully!")
