# LAJC-expungement

Project to help the Legal Aid and Justice Center with analysis to support efforts to expand criminal record expungement reform in Virginia.

For more details, see the [project page](https://www.codeforcville.org/lajc-expungement) on the Code for Charlottesville website. 

## Getting Started

Currently, data for this project is stored in a PostGreSQL DB only accessible from Code for Charlottesville's JupyterHub deployment, avaliable at tljh.codeforcville.org. 

If you do not yet have login access for JupyterHub, but you would like to get connected and help out, please reach out via the questions box at the bottom of the above project page. 

For examples of connecting to the database, see [the `/templates` folder](templates). 

- Example of connecting from a Python notebook: [templates/db_example_python.ipynb](templates/db_example_python.ipynb)

## About the Data

Data for this project comes originally from http://virginiacourtdata.org/, but has been processed and loaded into the main table `expunge`, which has ~9M records and about ~3M unique `person_id`'s. Each `person_id` represents an anonymized individual. 

Smaller samples of this data are available in the following tables: 
- `data_1k_sample`
- `data_10k_sample`
- `data_100k_sample`

These tables have been constructed such that they contain **all** records for any given `person_id` that they contain, so that they never contain a subset of the records related to a given ID. 

### Data Dictionary for `expunge` fields

| Column | Type | Description |
| ------ | ---- | ----------- |
| person_id | text | Unique identifier for an individual |
| HearingDate | date | Date of court hearing for record |
| CodeSection | text | Identifier for a particular section of Virginia Legal Code, corresponds to a category of offense |
| codesection | text | Relevant expungement code - describes whether and how any Virginia legal code affects an offense's `CodeSection`. |
| ChargeType | text | Classification of charge - 'Misdemeanor' or 'Felony' |
| chargetype | text | Duplicate of `ChargeType` |
| Class | text | Class of charge (1, 2, 3, etc), e.g. '1' = Class 1 Felony |
| DispositionCode | text | Final outcome of the arrest or prosecution - Guilty, Dismissed, etc. |
| disposition | text | Category of disposition as it relates to expungement qualification - 'Convicted', 'Deferred Dismissal', or 'Dismissed' |
| Plea | text | 'Guilty' plea or otherwise |
| Race | text | Race of person |
| Sex | text | Sex of person |
| fips | integer | Federal Information Processing Standards (FIPS) Code for the court where the case was filed — [here is a list of Virginia district court FIPS codes](https://github.com/bschoenfeld/virginia-court-data-analysis/blob/master/data/district_courts.csv) |
| convictions | text | Are there convictions on this person's record? - 'TRUE' or 'FALSE' |
| arrests | text | Are there arrests on this person's record **IN THE PAST 3 YEARS**? - 'TRUE' or 'FALSE' |
| felony10 | text | Does person have any felonies within the last 10 years? - 'TRUE' or 'FALSE' |
| sevenyear | text | Are there convictions of another kind within **7 years** from this record's disposition date? - 'TRUE' or 'FALSE' |
| tenyear | text | Are there convictions of another kind within **10 years** from this record's disposition date? - 'TRUE' or 'FALSE' |
| within7 | text | Disposition date is within **7 years** of the current date - 'TRUE' or 'FALSE' |
| within10 | text | Disposition date is within **10 years** of the current date - 'TRUE' or 'FALSE' |
| class1_2 | text | Person was convicted of a class 1 or 2 felony within the past 20 years - 'TRUE' or 'FALSE' |
| class3_4 | text | Person was ever convicted of a class 1 or 2 felony, or any other felony punishable by a life sentence - 'TRUE' or 'FALSE' |
| expungable | text | Eligibility of this record for expungement under proposed new Virginia legislation |
| old_expungable | text | Eligibility of this record for expungement under existing Virginia law |
| expungable_no_lifetimelimit | text | Eligibility of this record for expungement if the lifetime limit of 2 expungements is not considered. |
| reason | text | Plain english explanation of eligibility for expungement |
| sameday | text | Person was convicted of an offense that is not automatically expungable on the same day as this offense |
| lifetime | text | More than 2 lifetime expungements |

For more details about possible values for the original data from virginiacourtdata.org (which includes pretty much just the CamelCase fields), see [this blog post](https://virginiacourtdata.medium.com/virginia-court-data-fields-e224a9a41e15#.qkeo5r6ds) from Ben Schoenfeld. 
