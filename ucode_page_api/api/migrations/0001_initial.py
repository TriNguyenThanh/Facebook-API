# Generated for A.10 PostgreSQL tables.

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="Comment",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("comment_id", models.CharField(max_length=100, unique=True)),
                ("post_id", models.CharField(max_length=100)),
                ("message", models.TextField(blank=True, null=True)),
                ("intent", models.CharField(blank=True, max_length=50, null=True)),
                ("sentiment", models.CharField(blank=True, max_length=20, null=True)),
                ("status", models.CharField(default="received", max_length=20)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "db_table": "comments",
            },
        ),
        migrations.CreateModel(
            name="IdempotencyKey",
            fields=[
                ("command_id", models.CharField(max_length=100, primary_key=True, serialize=False)),
                ("processed_at", models.DateTimeField(auto_now_add=True)),
                ("status", models.CharField(max_length=20)),
            ],
            options={
                "db_table": "idempotency_keys",
            },
        ),
    ]
