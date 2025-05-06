// Python 解释器公共路径
const pythonScript = '/home/xintang/anaconda3/envs/fishball/bin/python3';  // Python 解释器路径（可根据实际路径修改）

const sharedConfig = {
    autorestart: true,
    restart_delay: 1000,
    max_restarts: 10,
    // max_memory_restart: '200M',
    watch: false,
    log_date_format: 'YYYY-MM-DD HH:mm Z',
    out_file: './logs/out.log',
    error_file: './logs/error.log',
    instances: 1,
    exec_mode: 'fork',
    script: pythonScript // 设置公共的 Python 解释器路径
  };
  
  module.exports = {
    apps: [
      Object.assign({
        name: 'f04',
        args: '/home/xintang/crypto/prod/alpha/factors_update/project/factors/zxt/f04_ts_net_size_pct_of_size.py',  // Python 脚本路径
      }, sharedConfig),
      Object.assign({
        name: 'f09',
        args: '/home/xintang/crypto/prod/alpha/factors_update/project/factors/zxt/f09_first_in_five.py',  // Python 脚本路径
      }, sharedConfig),
      Object.assign({
        name: 'f11',
        args: '/home/xintang/crypto/prod/alpha/factors_update/project/factors/zxt/f11_with_size.py',  // Python 脚本路径
      }, sharedConfig),
      Object.assign({
        name: 'f39',
        args: '/home/xintang/crypto/prod/alpha/factors_update/project/factors/zxt/f39_small_ba_amt_ratio.py',  // Python 脚本路径
      }, sharedConfig),
      Object.assign({
        name: 'f40',
        args: '/home/xintang/crypto/prod/alpha/factors_update/project/factors/zxt/f40_bidask_amount_ratio.py',  // Python 脚本路径
      }, sharedConfig),
      Object.assign({
        name: 'f41',
        args: '/home/xintang/crypto/prod/alpha/factors_update/project/factors/zxt/f41_bora_amount_ratio.py',  // Python 脚本路径
      }, sharedConfig),
      Object.assign({
        name: 'f42',
        args: '/home/xintang/crypto/prod/alpha/factors_update/project/factors/zxt/f42_small_bora_amt_ratio.py',  // Python 脚本路径
      }, sharedConfig),
      Object.assign({
        name: 'f51',
        args: '/home/xintang/crypto/prod/alpha/factors_update/project/factors/zxt/f51_ba_amt_ratio_filter_by_dist_out.py',  // Python 脚本路径
      }, sharedConfig),
      Object.assign({
        name: 'f55',
        args: '/home/xintang/crypto/prod/alpha/factors_update/project/factors/zxt/f55_hl_slope_ratio_diff_with_range_shortma.py',  // Python 脚本路径
      }, sharedConfig),
      Object.assign({
        name: 'f56',
        args: '/home/xintang/crypto/prod/alpha/factors_update/project/factors/zxt/f56_ba_amt_ratio_fsmall_by_dist_in.py',  // Python 脚本路径
      }, sharedConfig),
      Object.assign({
        name: 'f59',
        args: '/home/xintang/crypto/prod/alpha/factors_update/project/factors/zxt/f59_h_slope_ratio.py',  // Python 脚本路径
      }, sharedConfig),
      Object.assign({
        name: 'fpm_v0',
        args: '/home/xintang/crypto/prod/alpha/factors_update/project/factors/zxt/factors_for_portfolio_management_v0.py',  // Python 脚本路径
      }, sharedConfig),
    ]
  };