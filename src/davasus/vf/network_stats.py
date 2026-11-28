# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.network_stats                                      ║
# ║  « groups in a weighted network, and tests on pairs »            ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Communities by Newman's (2006) leading-eigenvector bisection    ║
# ║  with Kernighan-Lin refinement, in plain numpy. Pair matrices    ║
# ║  are compared by a Mantel test: heifers are relabelled, so       ║
# ║  every null matrix keeps the dependence between pairs that       ║
# ║  share an animal.                                                ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Community detection and node-permutation tests for pair matrices."""

from __future__ import annotations

import numpy as np
from scipy.stats import rankdata

from davasus.vf.constants import MODULARITY_NULL_DRAWS, PERMUTATION_CHUNK, PERMUTATIONS

MODULARITY_TOLERANCE: float = 1e-10


# ┌────────────────────────────────────────────────────────────┐
# │ Communities  « Newman 2006, repeated bisection »           │
# └────────────────────────────────────────────────────────────┘

def find_communities(weights: np.ndarray) -> tuple[np.ndarray, float]:
    """Community label per node and the modularity Q of the division.

    Args:
        weights: Symmetric non-negative pair weights; the diagonal is ignored.
    """
    adjacency = prepare_adjacency(weights)
    modularity_matrix, total = build_modularity_matrix(adjacency)
    pending, groups = [np.arange(len(adjacency))], []
    while pending:
        group = pending.pop()
        halves = bisect_group(modularity_matrix, group)
        if halves is None:
            groups.append(group)
        else:
            pending.extend(halves)
    labels = np.zeros(len(adjacency), dtype=int)
    for k, group in enumerate(sorted(groups, key=lambda g: g.min())):
        labels[group] = k
    return labels, compute_modularity(adjacency, labels)


def prepare_adjacency(weights: np.ndarray) -> np.ndarray:
    adjacency = np.clip(np.nan_to_num(np.asarray(weights, dtype=float)), 0.0, None)
    np.fill_diagonal(adjacency, 0.0)
    return adjacency


def build_modularity_matrix(adjacency: np.ndarray) -> tuple[np.ndarray, float]:
    """B_ij = A_ij - k_i k_j / 2m, and 2m."""
    degree = adjacency.sum(axis=1)
    total = float(degree.sum())
    if total <= 0:
        raise ValueError("network has no positive weights")
    return adjacency - np.outer(degree, degree) / total, total


def bisect_group(modularity_matrix: np.ndarray, group: np.ndarray) -> tuple[np.ndarray, np.ndarray] | None:
    """Split ``group`` in two if that raises modularity, else None."""
    if len(group) < 2:
        return None
    local = modularity_matrix[np.ix_(group, group)].copy()
    local -= np.diag(local.sum(axis=1))
    _, vectors = np.linalg.eigh(local)
    side = np.where(vectors[:, -1] >= 0, 1.0, -1.0)
    side = refine_bisection(local, side)
    if side @ local @ side <= MODULARITY_TOLERANCE or abs(side.sum()) == len(side):
        return None
    return group[side > 0], group[side < 0]


def refine_bisection(local: np.ndarray, side: np.ndarray) -> np.ndarray:
    """Move single nodes across while that raises modularity (Kernighan-Lin style)."""
    best = side @ local @ side
    is_improving = True
    while is_improving:
        is_improving = False
        for i in range(len(side)):
            side[i] = -side[i]
            gain = side @ local @ side
            if gain > best + MODULARITY_TOLERANCE:
                best, is_improving = gain, True
            else:
                side[i] = -side[i]
    return side


def compute_modularity(adjacency: np.ndarray, labels: np.ndarray) -> float:
    modularity_matrix, total = build_modularity_matrix(prepare_adjacency(adjacency))
    same = labels[:, None] == labels[None, :]
    return float(modularity_matrix[same].sum() / total)


def test_modularity(weights: np.ndarray, rng: np.random.Generator,
                    n_draws: int = MODULARITY_NULL_DRAWS) -> dict:
    """Q of the best division against networks with the pair weights shuffled."""
    adjacency = prepare_adjacency(weights)
    labels, observed = find_communities(adjacency)
    upper = np.triu_indices(len(adjacency), 1)
    null = np.empty(n_draws)
    for k in range(n_draws):
        shuffled = np.zeros_like(adjacency)
        shuffled[upper] = rng.permutation(adjacency[upper])
        null[k] = find_communities(shuffled + shuffled.T)[1]
    return {"n_communities": int(labels.max() + 1), "modularity": observed,
            "null_mean": float(null.mean()), "null_hi": float(np.quantile(null, 0.975)),
            "p": float((np.sum(null >= observed) + 1.0) / (n_draws + 1.0))}


# ┌────────────────────────────────────────────────────────────┐
# │ Mantel  « rank correlation of two pair matrices »          │
# └────────────────────────────────────────────────────────────┘

def test_matrix_correlation(predictor: np.ndarray, outcome: np.ndarray, rng: np.random.Generator,
                            n_permutations: int = PERMUTATIONS, control: np.ndarray | None = None) -> dict:
    """Spearman correlation of two pair matrices, heifers relabelled under the null.

    With ``control`` the rank trend on that matrix is removed from both first
    (partial Mantel test). Pairs with a NaN in any matrix are left out. The
    one-sided p asks whether the correlation is larger than under relabelling.
    """
    n = len(predictor)
    upper = np.triu_indices(n, 1)
    is_used = np.isfinite(predictor[upper]) & np.isfinite(outcome[upper])
    if control is not None:
        is_used &= np.isfinite(control[upper])
    ranks = np.full((n, n), np.nan)
    ranks[upper] = rankdata(predictor[upper], nan_policy="omit")
    ranks = np.fmax(ranks, ranks.T)
    target = _centre(rankdata(outcome[upper][is_used]))
    basis = None if control is None else _unit(_centre(rankdata(control[upper][is_used])))
    if basis is not None:
        target = target - (target @ basis) * basis
    observed = _correlate(ranks[upper][is_used][None, :], target, basis)[0]
    null = np.empty(n_permutations)
    for start in range(0, n_permutations, PERMUTATION_CHUNK):
        size = min(PERMUTATION_CHUNK, n_permutations - start)
        order = np.argsort(rng.random((size, n)), axis=1)
        relabelled = ranks[order[:, :, None], order[:, None, :]][:, upper[0], upper[1]]
        null[start:start + size] = _correlate(relabelled[:, is_used], target, basis)
    return {"rho": float(observed), "n_pairs": int(is_used.sum()), "null_mean": float(np.nanmean(null)),
            "null_hi": float(np.nanquantile(null, 0.95)),
            "p": float((np.sum(null >= observed) + 1.0) / (n_permutations + 1.0)),
            "p_two_sided": float((np.sum(np.abs(null) >= abs(observed)) + 1.0) / (n_permutations + 1.0))}


def _centre(values: np.ndarray) -> np.ndarray:
    return values - np.nanmean(values, axis=-1, keepdims=True)


def _unit(values: np.ndarray) -> np.ndarray:
    return values / np.linalg.norm(values)


def _correlate(rows: np.ndarray, target: np.ndarray, basis: np.ndarray | None) -> np.ndarray:
    """Pearson correlation of each row with ``target``, after removing ``basis`` from the rows."""
    rows = np.where(np.isfinite(rows), rows, np.nanmean(rows, axis=1, keepdims=True))
    rows = _centre(rows)
    if basis is not None:
        rows = rows - (rows @ basis)[:, None] * basis[None, :]
    with np.errstate(invalid="ignore", divide="ignore"):
        return (rows @ target) / (np.linalg.norm(rows, axis=1) * np.linalg.norm(target))


def test_partition_separation(labels: np.ndarray, together: np.ndarray, rng: np.random.Generator,
                              n_permutations: int = PERMUTATIONS) -> dict:
    """Do pairs of one community stay together more than pairs across communities.

    ``together`` is a pair matrix (NaN where unknown); labels are shuffled
    over heifers under the null.
    """
    is_known = np.isfinite(together) & ~np.eye(len(labels), dtype=bool)

    def difference(candidate: np.ndarray) -> float:
        same = candidate[:, None] == candidate[None, :]
        return float(together[is_known & same].mean() - together[is_known & ~same].mean())

    observed = difference(labels)
    null = np.array([difference(rng.permutation(labels)) for _ in range(n_permutations)])
    same = labels[:, None] == labels[None, :]
    return {"within": float(together[is_known & same].mean()), "between": float(together[is_known & ~same].mean()),
            "difference": observed, "null_hi": float(np.quantile(null, 0.95)),
            "p": float((np.sum(null >= observed) + 1.0) / (n_permutations + 1.0))}
