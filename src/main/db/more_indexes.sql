CREATE INDEX medium_release
  ON medium
  USING BTREE
  (release);

CREATE INDEX track_idx_medium
  ON track
  USING BTREE
  (medium, "position");

