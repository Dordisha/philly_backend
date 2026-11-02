# Approved logo — November 2, 2026

Owner decision: lock the green, gold and ivory direction. Copper/brown is superseded.
Reference: https://chatgpt.com/library?file_id=libfile_1ae87269c44081919811a7aa2670873f&account_id=personal

Keep the Liberty Bell with crack, magnifying glass, Philadelphia skyline in the full seal,
PHILLY SCAN name, PROPERTY FACTS & INSIGHTS tagline and D.D. 2026 in the full seal.
Use deep forest green, champagne gold accents and ivory. The simplified app icon keeps
only the bell and magnifier. Do not use the entire presentation board as an app icon.

Applied in preview.7 (build 10): the approved simplified icon is exported to
`mobile/assets/images/app-icon-green-gold.png` and every iOS AppIcon size.
Opaque, square green background; iOS supplies the corner mask. The launcher
generator now uses this dedicated master so it cannot restore the old black seal.
The full in-app seal is unchanged by this app-icon correction.

Asset extraction used the built-in image tool with the approved board as reference:
“Extract only the simplified upper-right app icon; preserve ivory Liberty Bell,
crack, gold magnifier and forest green; square edge-to-edge background; no text,
board, labels or rounded external corners.” ImageMagick exports required sizes.

In-app watermark: transparent green bell/gold magnifier, 4.5% opacity behind content, excluded from accessibility and touch handling. Generated from the same approved board with the instruction to extract the symbol onto transparency and use green for the bell.
