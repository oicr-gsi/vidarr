-- This change expands vidarr to accept multiple types of checksum
ALTER TABLE analysis
RENAME COLUMN file_md5sum TO file_checksum;

ALTER TABLE analysis
ADD COLUMN file_checksum_type varchar;

-- Since everything previous to this migration was md5sum, set everything to that type
UPDATE analysis SET file_checksum_type = 'md5sum';