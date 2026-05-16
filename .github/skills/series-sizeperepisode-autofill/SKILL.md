---
name: series-sizeperepisode-autofill
description: "Use when updating Backend/Data/movieCollection.nosql.json sizeperepisode from C:\\scripts\\Data\\SeriesStoragedetails.csv, including all filename normalization rules, typo fixes, manual alias exceptions, and movie exclusions discovered in this project."
---

# Series Size-Per-Episode Autofill

Apply this skill when asked to populate or refresh `sizeperepisode` values in:
- `Backend/Data/movieCollection.nosql.json`

From CSV source:
- `C:\scripts\Data\SeriesStoragedetails.csv`

## Canonical Mapping
- Join key in JSON: `data.filename`
- Join key in CSV: `folder_name`
- Fill value from CSV: `average_file_size_mb`
- Target field in JSON series items: `sizeperepisode`

## Mandatory Behavior
1. Parse JSON and CSV.
2. For each item where `metadataJson` contains `"Type": "series"`:
- If item is a known movie exception (list below), set `sizeperepisode` to `"N/A"` and skip matching.
- Otherwise, if `sizeperepisode` is blank, attempt match using exact filename first, then normalized matching.
3. Write JSON back.
4. Rebuild `Backend/Data/series_unmatched_filenames.txt` from still-blank series rows only (exclude entries set to `"N/A"`).

## Normalization Rules (apply to both JSON filename and CSV folder_name)
Apply in this order:
1. Strip release markers:
- `.(R)`, `.[R]`, `(R)`, `[R]`
- `.(X5)`, `.[X5]`, `(X5)`, `[X5]`
- `.(S5)`, `.[S5]`, `(S5)`, `[S5]`
2. Resolution typo fixes and removals:
- Convert `!080p`, `1090p`, `10880p` to `1080p`
- Remove `1080p`, `720p`, `480p`, `HD` tokens (with or without brackets/parentheses)
3. Remove `.US` substring.
4. Convert season zero-padding: `S01` -> `S1`, `S02` -> `S2`, etc.
5. Expand abbreviation:
- `TNG` -> `The.Next.Generation`
6. Remove year tokens in parentheses:
- `(2015)` style 4-digit years.
7. Ignore apostrophes.
8. Ignore extra spaces, parentheses, square brackets, and dots.

## Known Movie Exceptions (must not be treated as unresolved series)
Set `sizeperepisode` to `"N/A"` for these filenames:
- Apocalypse.Hitler.Pt.1[2011][1080p][X5][S][R]
- Apocalypse.Hitler.Pt.2[2011][1080p][X5][S][R]
- Watchmen[2009][DC][1080p][S][X5][R]
- Hitler.The.Rise.of.Evil[2003][1080p][X5][R]
- Houdini.Pt1[2014][1080p][R]
- Houdini.[2014][Pt2][720p][S]
- Love.And.Friendship[2016][1080p][X5]
- IT[1990][1080p][X5][S][R]
- World.On.A.Wire.Part1[1973][1080p][R]
- World.On.A.Wire.Part2[1973][1080p][R]
- Dominion.[2018][1080p]
- Monkey.Business[1931][1080p]
- V.The.Original.Miniseries[1983][1080p][X5][S][R]
- The.Stranger[2022][1080p][X5]
- Misanthrope[2023][720p][X5][S]
- Comte.de.Monte-Cristo[2024][1080p][X5][S]
- Game.Changer[2025][1080p][X5][S]
- Mrs.[2025][1080p][X5][S]
- Better.Man[2024[1080p][X5]
- Stolen.[2025][1080p][X5][S]
- Kingdom[2025][720p][X5][S]
- Sketch[2024][1080p][X5][S]
- The Girlfriend[2025][1080p][X5][S]
- Kennedy[2026][1080p][X5][S]

## Manual Alias Exceptions (authoritative left -> right)
Treat left-side JSON filename as matching right-side CSV folder_name:
- Erased.S1.(1080p).(X5) -> Eerased.S1.(1080p).(X5).(R)
- KardeÃ…Å¸.PayÃ„Â±.S1.(720p) -> KardeÅŸ.PayÄ±.S1.(720p)
- Sharp.Objects.(1080p).(X5) -> Sharp.Objects.S1.(1080p).(X5).(R)
- AranyÃƒÂ©let.aka.(Easy.Living).S1.(720p) -> AranyÃ©let.aka.(Easy.Living).S2.(720p)
- AranyÃƒÂ©let.aka.(Easy.Living).S2.(720p) -> AranyÃ©let.aka.(Easy.Living).S1.(720p)
- MerlÃƒÂ­.S1.(720p) -> MerlÃ­.S1.(720p)
- Time.S1.(720p).(X5) -> Time.2021.S1.(HD.(X5)
- La.Casa.De.Papel.S5.(Money.Heist).(720p).(X5) -> Money.Heist.S5.(720p).(X5)
- Cat.S1.(1080p).(X5) -> Cat.S1.(1080p).(S5)
- Afro.Samurai.S1.(1080p).(X5) -> Afro.Samura.S1.(1080p).(X5).(R)
- Reacher.S2.(1080p).(X5) -> Reacher.S2.(1090p).(X5)
- Severance.S2.(1080p).(X5) -> Severence.S2.(1080p).(X5)
- Rurouni.Kenshin.New.Kyoto.Arc.S1.(1080p.(X5) -> Rurouni.Kenshin.New.Kyoto.Arc.S1.(10880p.(X5)

## Validation Checklist
- JSON parses successfully after write.
- `sizeperepisode` exists for all true series rows:
- numeric/string value for matched rows
- empty string for still unmatched rows
- `"N/A"` for known movie exceptions
- `Backend/Data/series_unmatched_filenames.txt` only contains true series with blank `sizeperepisode`.
