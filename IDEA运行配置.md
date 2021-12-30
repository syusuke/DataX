# Idea 运行

## 运行类
```
com.alibaba.datax.core.Engine
```

## Maven 打包
```
mvn -U clean package assembly:assembly -Dmaven.test.skip=true
```

## VM Options
```
-Ddatax.home=/Users/kerryzhang/code/github-clone/DataX/target/datax/datax
```
改成: `core` 目录下的 `target/datax`


## Program args

```
-mode
standalone
-jobid
-1
-job
/Users/kerryzhang/code/github-clone/DataX/test-job/test.json
```
- **-job**: 为 job 文件路径