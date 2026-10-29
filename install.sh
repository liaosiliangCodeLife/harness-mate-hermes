#!/usr/bin/env bash
# HarnessMate Hermes 插件安装脚本
# 用法：bash install.sh
set -euo pipefail

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST_DIR="${HERMES_HOME:-$HOME/.hermes}/plugins/HarnessMate"

echo "==> 安装 HarnessMate Hermes 插件"
echo "    源目录: $SRC_DIR"
echo "    目标:   $DEST_DIR"

if [ -d "$DEST_DIR" ]; then
  BACKUP="$DEST_DIR.bak-$(date +%Y%m%d-%H%M%S)"
  echo "==> 已有插件，先备份到 $BACKUP"
  cp -R "$DEST_DIR" "$BACKUP"
fi

mkdir -p "$DEST_DIR"
for f in adapter.py __init__.py app_chat.py common.py file_upload.py plugin.yaml requirements.txt; do
  cp "$SRC_DIR/$f" "$DEST_DIR/$f"
done

echo "==> 文件已复制"
echo
echo "接下来（二选一）："
echo "  1) 在 Hermes 配置里填 plugins.HarnessMate.bot_id / bot_key"
echo "  2) 或者设置环境变量 HARNESS_MATE_BOT_ID / HARNESS_MATE_BOT_KEY"
echo "     平台「智能体 → 接入信息」里能找到这两个值。"
echo
echo "填好后重启 Hermes 网关使插件重新加载（hermes gateway status 可确认）。"
