# gordma

## messaging
### explicit processing and synchronization
- PostSend
- PostReceive

## write/read data
### without synchronization work with shared memory buffer
- PostWrite
- PostRead

## Infiniband Fabric Simulator

### Install
```bash
sudo apt-get install infiniband-diags
sudo apt-get install ibsim-utils

sudo apt-get install libibverbs-dev
sudo apt-get install ibverbs-utils /* ibv_devices, ibv_devinfo */
```

### Add
```bash
sudo rdma link add rxe0 type rxe netdev wlp2s0
```

### Delete
```bash
sudo rdma link del rxe0
```
