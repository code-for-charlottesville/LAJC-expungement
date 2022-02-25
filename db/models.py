import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import ARRAY


Base = declarative_base()


class Charges(Base):

    __tablename__ = 'charges'

    id = sa.Column(sa.BigInteger(), autoincrement=True, primary_key=True, unique=True)
    person_id = sa.Column(sa.BigInteger())
    hearing_date = sa.Column(sa.Date())
    code_section = sa.Column(sa.Text())
    charge_type = sa.Column(sa.Text())
    charge_class = sa.Column(sa.Text())
    disposition_code = sa.Column(sa.Text())
    plea = sa.Column(sa.Text())
    race = sa.Column(sa.Text())
    sex = sa.Column(sa.Text())
    fips = sa.Column(sa.Text())


class Runs(Base):

    __tablename__ = 'runs'

    id = sa.Column(sa.Text(), primary_key=True, unique=True)
    cutoff_date = sa.Column(sa.Date())
    covered_sections_a = sa.Column(ARRAY(sa.Text()))
    covered_sections_b = sa.Column(ARRAY(sa.Text()))
    covered_sections_b_misdemeanor = sa.Column(ARRAY(sa.Text()))
    excluded_sections_twelve = sa.Column(ARRAY(sa.Text()))
    years_since_arrest = sa.Column(sa.Integer())
    years_since_felony = sa.Column(sa.Integer())
    years_until_conviction_after_misdemeanor = sa.Column(sa.Integer())
    years_until_conviction_after_felony = sa.Column(sa.Integer())
    lifetime_rule = sa.Column(sa.Boolean())
    sameday_rule = sa.Column(sa.Boolean())


class Outcomes(Base):

    __tablename__ = 'outcomes'

    id = sa.Column(sa.BigInteger(), autoincrement=True, primary_key=True, unique=True)
    charge_id = sa.Column(sa.BigInteger(), sa.ForeignKey('charges.id'))
    run_id = sa.Column(sa.Text(), sa.ForeignKey('runs.id'))
    expungability = sa.Column(sa.Text())


class Features(Base):

    __tablename__ = 'features'

    id = sa.Column(sa.BigInteger(), autoincrement=True, primary_key=True, unique=True)
    charge_id = sa.Column(sa.BigInteger(), sa.ForeignKey('charges.id'))
    run_id = sa.Column(sa.Text(), sa.ForeignKey('runs.id'))
    disposition_type = sa.Column(sa.Text())
    charge_type = sa.Column(sa.Text())
    code_section_category = sa.Column(sa.Text())
    has_conviction = sa.Column(sa.Boolean())
    last_hearing_date = sa.Column(sa.Date())
    last_felony_conviction_date = sa.Column(sa.Date())
    next_conviction_date = sa.Column(sa.Date())
    last_hearing_delta = sa.Column(sa.Numeric())
    last_felony_conviction_delta = sa.Column(sa.Numeric())
    next_conviction_delta = sa.Column(sa.Numeric())
    from_present_delta = sa.Column(sa.Numeric())
    arrest_disqualifier = sa.Column(sa.Boolean())
    felony_conviction_disqualifier = sa.Column(sa.Boolean())
    next_conviction_disqualifier_after_misdemeanor = sa.Column(sa.Boolean())
    next_conviction_disqualifier_after_felony = sa.Column(sa.Boolean())
    pending_after_misdemeanor = sa.Column(sa.Boolean())
    pending_after_felony = sa.Column(sa.Boolean())
    is_class_1_or_2 = sa.Column(sa.Boolean())
    is_class_3_or_4 = sa.Column(sa.Boolean())
