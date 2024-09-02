<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->
<template>
  <div class="log-viewer-page">
    <div class="file-list">
      <h2>Log List - {{ serverId }}</h2>
      <ul>
        <li
          v-for="file in logFiles"
          :key="file.name"
          @click="selectFile(file)"
          :class="{ active: currentFile === file.name }"
        >
          <span class="file-name" :title="file.name">{{ file.name }}</span>
          <span class="file-size">{{ memFormatter(null, null, file.size) }}</span>
        </li>
      </ul>
    </div>
    <div class="file-content">
      <div v-if="currentFile" class="log-content" :key="key">
        <pre>{{ logContent }}</pre>
      </div>
      <div v-else class="no-file-selected">Please select a log file</div>
      <div v-if="currentFile" class="pagination-container">
        <el-pagination
          :current-page="currentPage"
          :page-size="pageSize * 1024 * 1024"
          :total="totalSize"
          @current-change="handlePageChange"
          layout="prev, pager, next"
        />
        <el-icon class="reload-icon" @click="reloadLog"><Refresh /></el-icon>
        <el-select
          v-model="pageSize"
          placeholder="Select page size"
          @change="handlePageSizeChange"
          class="page-size-select"
        >
          <el-option v-for="size in pageSizes" :key="size" :label="`${size}MB`" :value="size" />
        </el-select>
      </div>
    </div>
  </div>
</template>

<script>
import { ref, nextTick, onMounted } from 'vue'
import {
  getShuffleServerLogs,
  getCoordinatorLogs,
  getShuffleServerLogFile,
  getCoordinatorLogFile
} from '@/api/api'
import { ElMessage } from 'element-plus'
import { Refresh } from '@element-plus/icons-vue'
import { memFormatter } from '@/utils/common'

export default {
  name: 'LogViewerPage',
  components: { Refresh },
  props: {
    serverId: {
      type: String,
      required: true
    },
    targetAddress: {
      type: String,
      default: ''
    }
  },
  setup(props) {
    const logFiles = ref([])
    const currentFile = ref('')
    const logContent = ref('')
    const pageSize = ref(1)
    const pageSizes = [1, 5, 10, 20]
    const currentPage = ref(1)
    const totalSize = ref(0)
    const key = ref(0)
    const isPageSizeChanging = ref(false)

    onMounted(async () => {
      await fetchLogFiles()
    })

    const fetchLogFiles = async () => {
      try {
        let response
        if (props.targetAddress && props.targetAddress.length > 0) {
          const headers = { targetAddress: props.targetAddress }
          response = await getShuffleServerLogs({}, headers)
        } else {
          response = await getCoordinatorLogs()
        }
        if (response.status >= 200 && response.status < 300) {
          logFiles.value = Object.entries(response.data.data).map(([fileName, fileSize]) => ({
            name: fileName,
            size: fileSize
          }))
        } else {
          ElMessage.error('Failed to fetch log list')
        }
      } catch (err) {
        ElMessage.error('Failed to fetch log list: ' + err.message)
      }
    }

    const selectFile = (file) => {
      currentFile.value = file.name
      totalSize.value = file.size
      currentPage.value = 1
      loadLogContent()
    }

    const loadLogContent = async () => {
      if (!currentFile.value) return

      try {
        const offset = (currentPage.value - 1) * pageSize.value * 1024 * 1024
        const size = pageSize.value * 1024 * 1024
        let response
        if (props.targetAddress && props.targetAddress.length > 0) {
          const headers = { targetAddress: props.targetAddress }
          response = await getShuffleServerLogFile(currentFile.value, { offset, size }, headers)
        } else {
          response = await getCoordinatorLogFile(currentFile.value, { offset, size })
        }
        if (response.status >= 200 && response.status < 300) {
          logContent.value = response.data
          await nextTick()
          // update total pages
          const totalPages = Math.ceil(totalSize.value / (pageSize.value * 1024 * 1024))
          if (currentPage.value > totalPages) {
            currentPage.value = totalPages
            await loadLogContent() // reload to ensure correct content is displayed
          }
        } else {
          ElMessage.error('Failed to fetch log content')
        }
      } catch (err) {
        ElMessage.error('Failed to fetch log content: ' + err.message)
      }
    }

    const handlePageSizeChange = async (newSize) => {
      if (isPageSizeChanging.value) return
      isPageSizeChanging.value = true
      pageSize.value = newSize
      currentPage.value = 1 // reset to first page
      await loadLogContent()
      key.value++ // force re-render
      isPageSizeChanging.value = false
    }

    const handlePageChange = async (page) => {
      currentPage.value = page
      await loadLogContent()
    }

    const reloadLog = () => {
      loadLogContent()
    }

    return {
      logFiles,
      currentFile,
      logContent,
      pageSize,
      pageSizes,
      currentPage,
      totalSize,
      selectFile,
      handlePageSizeChange,
      handlePageChange,
      reloadLog,
      memFormatter,
      key
    }
  }
}
</script>

<style scoped>
.log-viewer-page {
  display: flex;
  height: 90vh;
}

.file-list {
  width: 300px;
  min-width: 250px;
  flex-shrink: 0;
  border-right: 1px solid #eee;
  overflow-y: auto;
  padding: 10px;
}

.file-list h2 {
  font-size: 1.2em;
  margin-bottom: 10px;
}

.file-list ul {
  list-style-type: none;
  padding: 0;
}

.file-list li {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 5px;
  cursor: pointer;
  font-size: 0.9em;
}

.file-list li:hover {
  background-color: #f0f0f0;
}

.file-list li.active {
  background-color: #e0e0e0;
}

.file-content {
  flex: 1;
  display: flex;
  flex-direction: column;
  padding: 10px;
  overflow: hidden;
}

.content-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 10px;
}

.reload-icon {
  cursor: pointer;
  font-size: 20px;
}

.reload-icon:hover {
  color: #409EFF;
}

.log-content {
  flex: 1;
  overflow-y: auto;
  background-color: #f5f5f5;
  padding: 10px;
  margin-bottom: 10px;
}

.log-content pre {
  white-space: pre-wrap;
  word-wrap: break-word;
  font-size: 0.9em;
}

.no-file-selected {
  display: flex;
  justify-content: center;
  align-items: center;
  height: 100%;
  font-size: 18px;
  color: #999;
}

@media (max-width: 768px) {
  .log-viewer-page {
    flex-direction: column;
  }

  .file-list {
    width: 100%;
    max-height: 30vh;
    border-right: none;
    border-bottom: 1px solid #eee;
  }

  .file-content {
    height: 60vh;
  }
}

.file-name {
  flex-grow: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  margin-right: 10px;
}

.pagination-container {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-top: 10px;
}

.page-size-select {
  width: 120px;
}

@media (max-width: 768px) {
  .pagination-container {
    flex-direction: column;
    align-items: flex-start;
  }

  .page-size-select {
    margin-top: 10px;
  }
}
</style>
