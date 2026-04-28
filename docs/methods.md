# DaVaSus — analysis methods

**Pipeline version:** 0.0.1
**Author:** Bart R. H. Geurten (University of Otago)
**Target audience:** lab partners reviewing the analysis chain;
companion document to `docs/database_structure.md`.

---

## 1. Scientific context

DaVaSus is the pasture-based mother-cow counterpart to the stall-based
DigiMuh dairy study. The **central question** is how the circadian
zeitgebers driving rumen-temperature, activity, rumination, and
locomotion shift when cows move from human-paced barn life
(fixed feeding and milking schedule) to extensive pasture (sun, own
grazing rhythm, operator-driven virtual-fence allocation).

We use four sensor channels per animal:

* **smaXtec rumen bolus** — temperature, activity, rumination, motility,
  drink-cycle index. Females only.
* **eShepherd virtual-fence collar** — GPS, IMU activity bins, fence
  distance, audio / pulse stimulus counters. All animals.
* **ATB Potsdam weather station** — air temperature, humidity, wind,
  rainfall, full radiation balance, soil heat flux, soil moisture.
* **Operator allocation log** — timestamped virtual-fence openings.
  *Pending delivery from ATB at the time of writing.*

The full schema is documented in `docs/database_structure.md`. This
document focuses on the per-analysis methods.


## 2. Pipeline overview

```
  davasus-ingest      (CSV → SQLite star schema)
        │
        ▼
  davasus-validate    (row counts, NULL rates, ranges, FK orphans)
        │
        ▼
  davasus-broken-stick   M4 — per-animal heat-stress THI threshold
        │  → figures/04_heat/broken_stick_results.csv
        ▼
  davasus-cosinor        M2 — per-animal-day 24 h cosinor on 4 signals,
        │                heat-stress-tagged from M4
        │  → figures/02_circadian/cosinor_fits.csv
        ▼
  davasus-zeitgeber      M3 — actograms, acrophase trajectory,
                         PLV-solar (and later PLV-feeding once the
                         allocation log arrives)
                       → figures/03_zeitgeber/...
```

Every CLI reads from the SQLite database in **read-only** mode. Outputs
are figures (PNG + editable-text SVG + the underlying CSV) and the
intermediate result tables. Figures live under `<repo>/figures/` and
are git-ignored.


## 3. Ingestion and validation

### 3.1 Ingestion

`davasus-ingest` streams the merged CSV (~15 GB, 61 M rows) and the raw
weather export (~28 K rows) into a star-schema SQLite database. Each
merged-CSV row produces one **eshepherd** fact row (always) and one
**smaxtec** fact row (skipped when all bolus columns are empty — i.e.
collar-only male animals). The eShepherd fence-distance sentinel
``-2147483647`` is stripped at ingest. Indices are built post-load.

### 3.2 Validation

`davasus-validate` runs five plausibility checks: table row counts,
NULL rates on key columns, value-range bounds (rumen temp 30–43 °C,
GPS lat ∈ [-90, 90], etc.), temporal coverage, and referential-
integrity orphans across all six FK relationships. Output is rendered
to stdout and optionally written as JSON.

Initial run on the 2024 dataset: zero FK orphans; ~0.04 % of GPS rows
have impossible lat/lon (357.9°) — a known eShepherd no-fix artefact
to be filtered with a plausibility polygon at analysis time.


## 4. M4 — Broken-stick heat-stress thresholds

### 4.1 Rationale

The temperature-humidity index (THI) at which an individual cow's
core body temperature begins to rise varies between animals.
Identifying per-animal breakpoints enables precision management and
provides ground-truth data for the literature population threshold
(Hoffmann et al. 2020; Neira et al. 2026: THI > 68.8 = mild stress
onset).

### 4.2 THI formulae

**Base — NRC (1971):**

$$
\mathrm{THI} = (1.8\,T + 32) - (0.55 - 0.0055\,\mathrm{RH}) \cdot (1.8\,T - 26.8)
$$

with $T$ in °C and RH as percent (0–100).

**Wind- and solar-adjusted — Mader–Gaughan:**

$$
\mathrm{THI}_{adj} = 4.51 + \mathrm{THI} - 1.992\,u + 0.0068\,R_{sw}
$$

with $u$ = wind speed (m s⁻¹) and $R_{sw}$ = incoming shortwave
radiation (W m⁻²). Mader et al. (2006); Gaughan et al. (2008).

DaVaSus uses the **Mader–Gaughan variant by default**; the NRC base
is retained as a sensitivity check (`--thi-mode nrc`).

### 4.3 Model

Continuous two-segment piecewise linear regression of rumen
temperature on THI:

$$
y = a + \beta_1\,x + (\beta_2 - \beta_1)\,\max(0,\,x - \mathrm{bp})
$$

with $x = \mathrm{THI}_{adj}$ and $y =$ `temp_without_drink_cycles`.
Biological constraint: $\beta_2 > \beta_1 \wedge \beta_2 > 0$ (rumen
temperature must rise above the breakpoint).

### 4.4 Estimation

Breakpoint search is a 200-point grid over the observed THI range
(with 5 % margin at each end) followed by a bounded scalar
minimisation (`scipy.optimize.minimize_scalar`). For each candidate
breakpoint the linear coefficients are solved by ordinary least
squares on the design matrix $[\mathbf{1},\,x,\,\max(0,\,x-\mathrm{bp})]$.

### 4.5 Population and filters

* Animals with at least one usable `smaxtec` row (excludes collar-only
  males).
* `temp_without_drink_cycles ∈ [30, 43]` °C at the row level.
* No time-of-day mask (DaVaSus has no milking; `temp_without_drink_cycles`
  already strips cold-drink artefacts).
* Per-animal, single-season fit (DaVaSus covers one season; DigiMuh's
  per-animal × per-year stratification does not apply).

### 4.6 Outputs

`figures/04_heat/`:

* `broken_stick_results.csv` — one row per animal: breakpoint,
  intercept, slope_below, slope_above, RMSE, R², n_points, success
  flag, reason.
* `breakpoint_distribution.{png,svg,csv}` — herd boxplot + Hoffmann
  2020 reference line.
* `example_fit_{best,median,worst}_animal_<id>.{png,svg,csv}` — three
  illustrative animal fits.

### 4.7 Live-data result

90 / 92 animals fit cleanly (24 s on the 12 GB DB). Median breakpoint
**68.5** (Mader THI), aligned with the literature threshold.
Slope-below ≈ 0 (thermoneutral), slope-above ≈ 0.05 °C / THI unit
(canonical). The two failures are inverted-V animals (rumen temp
*falls* with rising THI) — flagged for follow-up.


## 5. M2 — Per-animal-day cosinor

### 5.1 Rationale

A single 24 h cosine summarises a daily rhythm in three quickly
comparable numbers (Halberg 1959; Refinetti et al. 2007). DigiMuh runs
the same single-harmonic fit on rumen temperature, activity, and
rumination per animal-day; mirroring it in DaVaSus is what makes the
stall-vs-pasture comparison clean.

### 5.2 Model

$$
y(t) = M + A\,\cos\!\Big(\frac{2\pi t}{T} - \phi\Big) + \varepsilon
$$

with the period $T = 24$ h fixed. Three rhythm parameters per fit:

| Symbol | Meaning |
|---|---|
| $M$ — mesor | Rhythm-adjusted midline. |
| $A$ — amplitude | Half the peak-to-trough swing. |
| $\phi$ — acrophase | Hour-of-day at which the cosine peaks. |

### 5.3 Estimation

Closed-form via the discrete Fourier transform at frequency $1/T$:

$$
C = \overline{y\cos(2\pi t/T)},\quad
S = \overline{y\sin(2\pi t/T)},\quad
M = \bar{y}
$$

then $A = 2\sqrt{C^2 + S^2}$ and
$\phi_{\text{hours}} = \frac{T}{2\pi}\,\operatorname{atan2}(S, C)$,
wrapped into $[0, T)$. No iterative optimisation is needed.

### 5.4 Signals

Four channels, all aggregated to hourly means before fitting:

| Tag | Source column | Source table |
|---|---|---|
| `rumen_temp` | `temp_without_drink_cycles` | `smaxtec` |
| `act_index` | `act_index` | `smaxtec` |
| `rum_index` | `rum_index_x` | `smaxtec` |
| `imu_activity` | `Σ imu_tick_*mg` | `eshepherd` |

### 5.5 Heat-stress tagging

Each `(animal_id, date)` is tagged **heat-stress** if the day's
maximum Mader-Gaughan THI exceeded the animal's M4 breakpoint;
otherwise **cool**. The tag is carried on every cosinor result row
so downstream slicing (acrophase distributions, actograms) is free.

### 5.6 Hierarchical 24 h profile

The headline 24 h profile plots use a **two-stage aggregation**:

1. For each animal × heat-status × hour, compute the per-animal mean.
2. Across animals at each hour, compute the mean and the SEM
   ($\mathrm{SD}/\sqrt{n}$).

`N` reported on the figure is the number of contributing animals,
not the pooled observation count — animals are the experimental unit
and the inferential SEM should reflect between-animal variability.
At hour 12 the pooled SEM was 0.0036 °C (n = 9 584 observations);
the hierarchical SEM is 0.0126 °C (n = 90 animals) — 3.5× wider and
correct.

The observation-pooled plots are kept as a secondary figure
(`profile_24h_<signal>.{png,svg,csv}`) because they remain useful for
spotting outlier hours.

### 5.7 Outputs

`figures/02_circadian/`:

* `cosinor_fits.csv` — one row per `(animal_id, date, signal)` with
  the three rhythm parameters, ``relative_amplitude``, ``n_hours``,
  ``success`` flag, ``reason``, and ``heat_stress_day``.
* `profile_24h_<signal>_per_animal.{png,svg,csv}` — **hierarchical
  headline plot** (mean ± SEM across animals, cool vs heat-stress).
* `profile_24h_<signal>.{png,svg,csv}` — observation-pooled secondary.
* `amplitude_boxplot_<signal>.{png,svg,csv}` — daily amplitude
  distribution stratified by heat-stress status.

### 5.8 Live-data result

24 364 animal-days × 4 signals = 97 456 fits in 119 s; 92.6 % success.

Heat-stress vs cool day (rumen temp, medians): mesor 39.21 vs 39.04 °C,
amplitude **0.38 vs 0.26 °C**, acrophase 16:33 vs 16:58 UTC. The
afternoon peak is *more pronounced* under heat load and slightly
phase-advanced, consistent with the literature (Lees et al. 2019).
Acrophase by month: April–October peak at 17:00 ± 1 h UTC; November–
December collapse to noise-driven uniform-random estimates due to
low winter amplitude (follow-up: amplitude-prefilter).


## 6. M3 — Zeitgeber attribution (visual core)

### 6.1 Rationale

A rhythm exists; the open question is *what entrains it*. Plotted in
wall-clock time, a sun-locked rhythm slopes seasonally; in
zeitgeber-time (relative to sunrise) it lies flat. Plotted in
zeitgeber-time, a feeding-locked rhythm slopes opposite to the
sunrise curve. The actogram and acrophase trajectory are the two
canonical chronobiology visualisations for this question (Aschoff,
Refinetti 2006).

### 6.2 Sunrise / sunset from `pot_slr_rad_avg`

The Campbell weather logger reports the **astronomically computed**
potential solar radiation. It is negative at night and positive
during the day. We extract sunrise and sunset per date as the
zero-crossings of this signal, refined by linear interpolation
between the bracketing samples. No external astronomy library is
needed — the logger has already done the geometry.

### 6.3 Actogram heatmaps

For each of the four signals, the herd-median value at every
`(date, hour-of-day)` cell is rendered as a heatmap (Y = date,
X = hour 0–24, colour = median value). Sunrise / solar-noon / sunset
curves are overlaid in black so the eye can read entrainment off the
slope of the bright bands relative to the photic reference.

### 6.4 Acrophase trajectory

For the rumen-temperature signal we plot **acrophase vs date** with a
faint per-animal trace, the herd-median trace bold, and the sunrise /
sunset curves overlaid. This single panel tells the entrainment
story: a herd-median that rides the sunrise curve is sun-locked; a
flat herd-median is clock-locked.

### 6.5 Phase-locking value (PLV)

For each animal we compute

$$
\mathrm{PLV}_\text{solar} =
\Big| \frac{1}{N_\text{days}} \sum_{d=1}^{N_\text{days}} e^{i\,(\theta_\text{rumen}(d) - \theta_\text{solar}(d))} \Big|
$$

where $\theta_\text{rumen}(d)$ is the rumen-temperature acrophase on
day $d$ (in radians) and $\theta_\text{solar}(d)$ is the solar-noon
phase from §6.2. PLV ∈ [0, 1]; values near 1 mean the cow's daily
peak rides the sun.

Once the operator allocation log arrives we add a second term

$$
\mathrm{PLV}_\text{feeding}
$$

between the rumen-temperature acrophase and the timestamps of the
fence-opening pulse train, plus a variance-decomposition linear
model

$$
T_\text{rumen}(t) = \beta_0
+ \beta_\text{solar}\,[\sin(2\pi t / 24_\text{solar}),\,\cos(\cdot)]
+ \beta_\text{feed}\,[\sin(2\pi t / T_\text{feed}),\,\cos(\cdot)]
+ \beta_\text{THI}\,\mathrm{THI}(t)
+ \varepsilon
$$

reporting per-animal $R^2_\text{solar}$ and $R^2_\text{feeding}$ as
the compact zeitgeber-strength metric.

### 6.6 Outputs

`figures/03_zeitgeber/`:

* `actogram_<signal>.{png,svg,csv}` — four signals.
* `acrophase_trajectory_rumen_temp.{png,svg,csv}`.
* `plv_solar.{png,svg,csv}`.

### 6.7 Live-data result

Median PLV-solar = **0.40** across 90 animals (~248 days each,
range 0.21–0.67). Pasture rhythm is **moderately** sun-locked — not
random, not perfect entrainment. The expected interpretation
requires the PLV-feeding number for comparison; until the
allocation log arrives, this is the standalone baseline.


## 7. Figure conventions

All figures follow the project convention:

* **PNG** raster preview at 300 dpi.
* **SVG** vector with **editable text** (font glyphs, not paths)
  for downstream typesetting.
* **CSV** with the data behind the figure, in long format, so any
  reviewer can re-plot without re-running the pipeline.

Colours come from the Wong (2011) / Okabe-Ito colourblind-safe palette,
mapped semantically per variable in `src/davasus/constants.py`. Same
biological signal → same colour across every figure in the project.


## 8. References

* **Aschoff, J.** (1981). *Biological Rhythms.* Plenum Press, New York.
* **Davies, R. B.** (1987). Hypothesis testing when a nuisance
  parameter is present only under the alternative. *Biometrika* 74,
  33–43.
* **Gaughan, J. B., Mader, T. L., Holt, S. M., Lisle, A.** (2008). A
  new heat load index for feedlot cattle. *J. Anim. Sci.* 86(1),
  226–234. https://doi.org/10.2527/jas.2007-0305
* **Halberg, F.** (1959). Physiologic 24-hour periodicity: general and
  procedural considerations with reference to the adrenal cycle.
  *Z. Vitam. Horm. Fermentforsch.* 10, 225–296.
* **Hoffmann, G., Herbut, P., Pinto, S., Heinicke, J., Kuhla, B.,
  Amon, T.** (2020). Animal-related, non-invasive indicators for
  determining heat stress in dairy cows. *Biosystems Engineering* 199,
  83–96.
* **Lees, A. M., Lees, J. C., Lisle, A. T., Sullivan, M. L., Gaughan,
  J. B.** (2019). Effect of heat stress on rumen temperature of three
  breeds of cattle. *Int. J. Biometeorol.* 63(2), 211–219.
* **Mader, T. L., Davis, M. S., Brown-Brandl, T.** (2006).
  Environmental factors influencing heat stress in feedlot cattle.
  *J. Anim. Sci.* 84(3), 712–719. https://doi.org/10.2527/2006.843712x
* **Muggeo, V. M. R.** (2003). Estimating regression models with
  unknown break-points. *Statistics in Medicine* 22, 3055–3071.
* **Muggeo, V. M. R.** (2016). Testing with a nuisance parameter
  present only under the alternative: a score-based approach with
  application to segmented modelling. *J. Stat. Comput. Simul.*
  86(15), 3059–3067.
* **Neira, P., Geurten, B., Hoffmann, G., et al.** (2026). *In
  preparation* — individual heat-stress thresholds in dairy cattle
  via broken-stick regression of rumen temperature.
* **NRC** (1971). *A guide to environmental research on animals.*
  National Academy of Sciences, Washington DC.
* **Refinetti, R.** (2006). *Circadian Physiology* (2nd edn). CRC
  Press / Taylor & Francis.
* **Refinetti, R., Lissen, G. C., Halberg, F.** (2007). Procedures
  for numerical analysis of circadian rhythms. *Biological Rhythm
  Research* 38(4), 275–325.
* **Wong, B.** (2011). Points of view: Color blindness. *Nature
  Methods* 8(6), 441. https://doi.org/10.1038/nmeth.1618
