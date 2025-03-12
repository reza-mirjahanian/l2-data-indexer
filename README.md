

# L2 Data Indexer

🥎 Please check the Git branches.


https://sepolia.etherscan.io/address/0x761d53b47334bee6612c0bd1467fb881435375b2

## Getting started



To stop the running code and check if anything has been written to the database, execute:

```bash
go run cli/run.go
```

### Using Docker

```bash
make docker
make run
```

If Docker is already built and there are no code changes, simply run:

```bash
make rund
make run
```

## 🎬 Demo


## ✅ Done
* [x] maxPerRequest check.
* [x] maxPerAddress check.
* [x] RateLimiter  check.
* [x] Enable or disable the Faucet.
* [x] Put capacity limit for the Faucet.

-------------------
## 📝 Todo List

#### Development Tasks
-   [ ] Implement unit tests with proper coverage
-   [ ] Add end-to-end (e2e) tests and use [SimApp](https://docs.cosmos.network/v0.52/learn/advanced/simulation)
-   [ ] Set up Docker or Kubernetes containerization
-   [ ] Configure GitHub Actions for CI/CD
-   [ ] Implement git hooks
-   [ ] Add comprehensive logging system ( [Telemetry](https://docs.cosmos.network/main/learn/advanced/telemetry) )
-   [ ] Implement event publishing mechanism ( [ctx.EventManager().EmitEvent](https://docs.cosmos.network/main/learn/advanced/events) )


#### Security Tasks
- [ ] Conduct a thorough security audit of the codebase
- [ ] Implement proper input validation and sanitization
- [ ] Implement proper error handling

