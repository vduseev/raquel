from alembic import op
import sqlalchemy as sa


def upgrade() -> None:
    op.create_table('jobs',
    sa.Column('id', sa.Uuid(), nullable=False),
    sa.Column('queue', sa.String(), nullable=False),
    sa.Column('payload', sa.String(), nullable=True),
    sa.Column('status', sa.String(length=30), nullable=False),
    sa.Column('max_age', sa.BigInteger(), nullable=True),
    sa.Column('max_retry_count', sa.Integer(), nullable=True),
    sa.Column('min_retry_delay', sa.Integer(), nullable=True),
    sa.Column('max_retry_delay', sa.Integer(), nullable=True),
    sa.Column('backoff_base', sa.Integer(), nullable=True),
    sa.Column('enqueued_at', sa.BigInteger(), nullable=False),
    sa.Column('scheduled_at', sa.BigInteger(), nullable=False),
    sa.Column('attempts', sa.Integer(), nullable=False),
    sa.Column('error', sa.String(), nullable=True),
    sa.Column('error_trace', sa.String(), nullable=True),
    sa.Column('claimed_by', sa.String(), nullable=True),
    sa.Column('claimed_at', sa.BigInteger(), nullable=True),
    sa.Column('finished_at', sa.BigInteger(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_jobs_claimed_by'), 'jobs', ['claimed_by'], unique=False)
    op.create_index(op.f('ix_jobs_queue'), 'jobs', ['queue'], unique=False)
    op.create_index(op.f('ix_jobs_scheduled_at'), 'jobs', ['scheduled_at'], unique=False)
    op.create_index(op.f('ix_jobs_status'), 'jobs', ['status'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_jobs_status'), table_name='jobs')
    op.drop_index(op.f('ix_jobs_scheduled_at'), table_name='jobs')
    op.drop_index(op.f('ix_jobs_queue'), table_name='jobs')
    op.drop_index(op.f('ix_jobs_claimed_by'), table_name='jobs')
    op.drop_table('jobs')
