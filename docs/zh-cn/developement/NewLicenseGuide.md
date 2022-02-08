## 如何添加新的许可证指南

如果你有任何新的 Jar 二进制包添加到你的 PR 中，你需要按照以下步骤设置许可证

1. 声明于 `tools/dependencies/konw-dependencies.txt`
2. 添加相应的 License 文件在 `seatunnel-dist/release-docs/licenses`, 如果是标准的 Apache License, 则不需要添加。
3. 在 `seatunnel-dist/release-docs/LICENSE` 中添加相应的语句
4. 在 `seatunnel-dist/release-docs/NOTICE` 中添加相应的语句

如果你想了解更多关于许可证策略的信息，你可以阅读提交指南中的[License Notice](https://seatunnel.apache.org/community/submit_guide/license)
