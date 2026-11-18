# Terraform (owner: Kanad)

Target layout — to be built in Week 3:

| File            | Resources                                                  |
|-----------------|------------------------------------------------------------|
| `main.tf`       | provider, backend, common tags                             |
| `network.tf`    | VPC, public/private subnets, security groups               |
| `ecr.tf`        | ECR repos for `gateway` and `mockllm`                      |
| `ecs.tf`        | ECS cluster, Fargate services/tasks, autoscaling policies  |
| `alb.tf`        | ALB, target group, health check on `/healthz`              |
| `redis.tf`      | ElastiCache Redis                                          |
| `queue.tf`      | SQS (if we pick SQS over Redis Streams)                    |
| `monitoring.tf` | CloudWatch log groups, billing alarm                       |

Cost rules: smallest task sizes, **`terraform destroy` after every session**, billing alert on.
