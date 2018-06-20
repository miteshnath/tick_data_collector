from bittrex import db
# from sqlalchemy import UniqueConstraint


class oneMinTick(db.Model):
    __tablename__ = 'oneMinTick'
    id = db.Column(db.Integer, primary_key=True)
    Timestamp = db.Column(db.Integer, nullable=False)
    Market = db.Column(db.String(15), nullable=False)
    Open = db.Column(db.Float, nullable=False)
    Close = db.Column(db.Float, nullable=False)
    High = db.Column(db.Float, nullable=False)
    Low = db.Column(db.Float, nullable=False)
    Volume = db.Column(db.Float, nullable=False)
    BaseVolume = db.Column(db.Float, nullable=False)
    __table_args__ = (db.UniqueConstraint('Timestamp', 'Market', name='uix_1'), )

    
    def __repr__(self):
        return '<Market-Timestamp: {}={}>'.format(self.name, self.Timestamp)


class fiveMinTick(db.Model):
    __tablename__ = 'fiveMinTick'
    id = db.Column(db.Integer, primary_key=True)
    Timestamp = db.Column(db.Integer, nullable=False)
    Market = db.Column(db.String(15), nullable=False)
    Open = db.Column(db.Float, default=0.0)
    Close = db.Column(db.Float, default=0.0)
    High = db.Column(db.Float, default=0.0)
    Low = db.Column(db.Float, default=0.0)
    Volume = db.Column(db.Float, default=0.0)
    __table_args__ = (db.UniqueConstraint('Timestamp', 'Market', name='uix_5'), )

    def __repr__(self):
        return '<Market-Timestamp: {}={}>'.format(self.name, self.Timestamp)


class quaterHourTick(db.Model):
    __tablename__ = 'quaterHourTick'
    id = db.Column(db.Integer, primary_key=True)
    Timestamp = db.Column(db.Integer, nullable=False)
    Market = db.Column(db.String(15), nullable=False)
    Open = db.Column(db.Float, default=0.0)
    Close = db.Column(db.Float, default=0.0)
    High = db.Column(db.Float, default=0.0)
    Low = db.Column(db.Float, default=0.0)
    Volume = db.Column(db.Float, default=0.0)
    __table_args__ = (db.UniqueConstraint('Timestamp', 'Market', name='uix_15'), )

    def __repr__(self):
        return '<Market-Timestamp: {}={}>'.format(self.name, self.Timestamp)


class halfHourTick(db.Model):
    __tablename__ = 'halfHourTick'
    id = db.Column(db.Integer, primary_key=True)
    Timestamp = db.Column(db.Integer, nullable=False)
    Market = db.Column(db.String(15), nullable=False)
    Open = db.Column(db.Float, default=0.0)
    Close = db.Column(db.Float, default=0.0)
    High = db.Column(db.Float, default=0.0)
    Low = db.Column(db.Float, default=0.0)
    Volume = db.Column(db.Float, default=0.0)
    __table_args__ = (db.UniqueConstraint('Timestamp', 'Market', name='uix_30'), )

    def __repr__(self):
        return '<Market-Timestamp: {}={}>'.format(self.name, self.Timestamp)


class oneHourTick(db.Model):
    __tablename__ = 'oneHourTick'
    id = db.Column(db.Integer, primary_key=True)
    Timestamp = db.Column(db.Integer, nullable=False)
    Market = db.Column(db.String(15), nullable=False)
    Open = db.Column(db.Float, default=0.0)
    Close = db.Column(db.Float, default=0.0)
    High = db.Column(db.Float, default=0.0)
    Low = db.Column(db.Float, default=0.0)
    Volume = db.Column(db.Float, default=0.0)
    __table_args__ = (db.UniqueConstraint('Timestamp', 'Market', name='uix_60'), )

    def __repr__(self):
        return '<Market-Timestamp: {}={}>'.format(self.name, self.Timestamp)
