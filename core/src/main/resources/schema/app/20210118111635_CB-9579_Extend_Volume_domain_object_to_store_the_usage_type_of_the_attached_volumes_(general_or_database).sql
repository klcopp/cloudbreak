-- // CB-9579 Extend Volume domain object to store the usage type of the attached volumes (general or database)
-- Migration SQL that makes the change goes here.

ALTER TABLE volumetemplate
    ADD COLUMN IF NOT EXISTS usagetype VARCHAR(255);

-- //@UNDO
-- SQL to undo the change goes here.

ALTER TABLE volumetemplate
    DROP COLUMN IF EXISTS usagetype;
