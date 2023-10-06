CREATE TABLE credit (
    indx INTEGER,
    age INTEGER,
    sex VARCHAR,
    job INTEGER,
    housing VARCHAR,
	saving_accounts VARCHAR,
	checking_accounts VARCHAR,
	credit_amount INTEGER,
	duration INTEGER,
	purpose VARCHAR
);

ALTER TABLE credit
DROP COLUMN indx;

select * from credit


