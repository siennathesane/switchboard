//! Topic-exchange matching (§3.1.3.3).
//!
//! Routing keys and patterns are dot-delimited words. In patterns, `*`
//! matches exactly one word and `#` matches zero or more words. The spec's
//! example: `*.stock.#` matches `usd.stock` and `eur.stock.db` but not
//! `stock.nasdaq`.

/// Match a routing key against a pattern, both pre-split on `.`.
///
/// Iterative two-pointer scan with backtracking over the last `#`, which
/// keeps matching O(pattern × key) without recursion blowups on pathological
/// patterns like `#.#.#.#`.
pub fn words_match(pattern: &[&str], key: &[&str]) -> bool {
    let (mut p, mut k) = (0usize, 0usize);
    // Position of a `#` in the pattern and the key position it was tried at,
    // for backtracking.
    let (mut star_p, mut star_k): (Option<usize>, usize) = (None, 0);

    while k < key.len() {
        if p < pattern.len() {
            match pattern[p] {
                "#" => {
                    star_p = Some(p);
                    star_k = k;
                    p += 1;
                    // `#` can match zero words; try that first.
                    continue;
                }
                "*" => {
                    p += 1;
                    k += 1;
                    continue;
                }
                w if w == key[k] => {
                    p += 1;
                    k += 1;
                    continue;
                }
                _ => {}
            }
        }
        // Mismatch (or pattern exhausted): backtrack into the last `#`,
        // letting it swallow one more key word.
        if let Some(sp) = star_p {
            star_k += 1;
            p = sp + 1;
            k = star_k;
        } else {
            return false;
        }
    }

    // Key exhausted: only `#`s may remain in the pattern.
    pattern[p..].iter().all(|w| *w == "#")
}

/// Split a routing key or pattern into words. Keys are the raw octets of the
/// shortstr; empty segments are preserved (`a..b` is a three-word key) —
/// the grammar allows any words delimited by dots.
pub fn split(s: &str) -> Vec<&str> {
    if s.is_empty() {
        // An empty routing key is one empty word? No: it is zero words —
        // matching a pattern of `#` (zero or more) and *nothing else*.
        // Represent that as zero words.
        Vec::new()
    } else {
        s.split('.').collect()
    }
}

/// Convenience: match pattern and key strings.
pub fn matches(pattern: &str, key: &str) -> bool {
    words_match(&split(pattern), &split(key))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(pattern: &str, key: &str) -> bool {
        matches(pattern, key)
    }

    #[test]
    fn spec_example() {
        // From §3.1.3.3: `*.stock.#` matches `usd.stock` and `eur.stock.db`
        // but not `stock.nasdaq`.
        assert!(m("*.stock.#", "usd.stock"));
        assert!(m("*.stock.#", "eur.stock.db"));
        assert!(!m("*.stock.#", "stock.nasdaq"));
    }

    #[test]
    fn exact_and_single_level() {
        assert!(m("a.b.c", "a.b.c"));
        assert!(!m("a.b.c", "a.b"));
        assert!(!m("a.b", "a.b.c"));
        assert!(m("*", "anything"));
        assert!(!m("*", "a.b"));
        assert!(m("a.*", "a.b"));
        assert!(!m("a.*", "a.b.c"));
    }

    #[test]
    fn hash_matches_zero_or_more() {
        assert!(m("#", "a.b.c"));
        assert!(m("#", ""));
        assert!(m("a.#", "a"));
        assert!(m("a.#", "a.b.c"));
        assert!(m("#.b", "b"));
        assert!(m("#.b", "x.y.b"));
        // '#' swallows both x and c here: a, <x c>, b.
        assert!(m("a.#.b", "a.x.c.b"));
        assert!(!m("a.#.b", "a.x.c.d"));
        assert!(m("a.#.b", "a.x.b"));
        assert!(m("a.#.b", "a.b"));
    }

    #[test]
    fn hash_star_combined() {
        assert!(m("#.a.*.b.#", "q.a.z.b"));
        assert!(m("#.a.*.b.#", "a.z.b"));
        assert!(!m("#.a.*.b.#", "a.b"));
        // a, <one word>, b must be contiguous.
        assert!(m("#.a.*.b.#", "x.y.a.z.b.w.q"));
        assert!(!m("#.a.*.b.#", "x.y.a.z.z.b.w.q"));
    }

    #[test]
    fn empty_keys_and_patterns() {
        assert!(m("", ""));
        assert!(m("#", ""));
        assert!(!m("*", ""));
        assert!(!m("", "a"));
    }

    #[test]
    fn pathological_backtracking_terminates() {
        let pattern = "#.#.#.#.#.z";
        let key = "a.b.c.d.e.f.g.h.i.j";
        assert!(!m(pattern, key));
        assert!(m("#.#.#.#.#.j", key));
    }

    #[test]
    fn words_preserve_empty_segments() {
        assert_eq!(split("a..b"), vec!["a", "", "b"]);
        assert!(split("").is_empty());
        assert!(m("a.#", "a..b"));
        assert!(m("a..b", "a..b"));
        assert!(!m("a.b", "a..b"));
    }
}
