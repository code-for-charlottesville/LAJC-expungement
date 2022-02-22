import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Charges(Base):

    __tablename__ = 'charges'

    id = sa.Column(sa.Integer(), autoincrement=True, primary_key=True, unique=True)
    person_id = sa.Column(sa.BigInteger())
    hearing_date = sa.Column(sa.Date())
    code_section = sa.Column(sa.Text())
    charge_type = sa.Column(sa.Text())
    charge_class = sa.Column(sa.Text())
    disposition_code = sa.Column(sa.Text())
    plea = sa.Column(sa.Text())
    race = sa.Column(sa.Text())
    sex = sa.Column(sa.Text())
    fips = sa.Column(sa.Integer())


class Features(Base):

    __tablename__ = 'features'

    charge_id = sa.Column(sa.Integer(), sa.ForeignKey('charges.id'))
    run_id = sa.Column(sa.Text(), sa.ForeignKey('outcomes.run_id'))
    disposition = sa.Column(sa.Text())
    chargetype = sa.Column(sa.Text())
    codesection = sa.Column(sa.Text())
    convictions = sa.Column(sa.Boolean())
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
    class1_2 = sa.Column(sa.Boolean())
    class3_4 = sa.Column(sa.Boolean())


class Outcomes(Base):

    __tablename__ = 'outcomes'

    charge_id = sa.Column(sa.Integer(), sa.ForeignKey('charges.id'))
    run_id = sa.Column(sa.Text())
    expungability = sa.Column(sa.Text())
