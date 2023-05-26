"""Initial migration.

Revision ID: ab8f9cceb464
Revises: 
Create Date: 2023-05-09 03:51:04.781845

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ab8f9cceb464'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('weather_forecast',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('latitude', sa.Numeric(precision=50, scale=25), nullable=True),
    sa.Column('longitude', sa.Numeric(precision=50, scale=25), nullable=True),
    sa.Column('updated_date_utc', sa.DateTime(), nullable=True),
    sa.Column('forecast_date_utc', sa.DateTime(), nullable=True),
    sa.Column('air_temperature', sa.Numeric(precision=50, scale=25), nullable=True),
    sa.Column('soil_temperature', sa.Numeric(precision=50, scale=25), nullable=True),
    sa.Column('volumetric_soil_moisture_content', sa.Numeric(precision=50, scale=25), nullable=True),
    sa.Column('rainfall_boolean', sa.Boolean(), nullable=True),
    sa.Column('precipitation_rate', sa.Numeric(precision=50, scale=25), nullable=True),
    sa.Column('relative_humidity', sa.Numeric(precision=50, scale=25), nullable=True),
    sa.Column('dew_point_temperature', sa.Numeric(precision=50, scale=25), nullable=True),
    sa.Column('pressure_reduced_to_msl', sa.Numeric(precision=50, scale=25), nullable=True),
    sa.Column('pressure', sa.Numeric(precision=50, scale=25), nullable=True),
    sa.Column('wind_speed', sa.Numeric(precision=50, scale=25), nullable=True),
    sa.Column('total_cloud_cover', sa.Numeric(precision=50, scale=25), nullable=True),
    sa.Column('forecast_date_local', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    with op.batch_alter_table('weather_forecast', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_weather_forecast_id'), ['id'], unique=False)

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('weather_forecast', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_weather_forecast_id'))

    op.drop_table('weather_forecast')
    # ### end Alembic commands ###
