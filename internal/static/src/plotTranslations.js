import { getLanguage } from "./i18n.js";

export const plotLabels = {
  "CGO Calls": "CGO 调用",
  "CPU (Overall)": "CPU（总体）",
  "CPU (Garbage Collector)": "CPU（垃圾回收器）",
  "CPU (Scavenger)": "CPU（内存归还）",
  "GC Memory Summary": "GC 内存概览",
  "GC Cleanups Queue": "GC 清理函数队列",
  "GC Finalizers Queue": "GC 终结器队列",
  "Completed GC Cycles": "已完成的 GC 周期",
  "GC Scan": "GC 扫描",
  "Goroutines": "协程",
  "Goroutines stack starting size": "协程初始栈大小",
  "Heap Allocation & Free Rates": "堆分配与释放速率",
  "Heap (details)": "堆内存详情",
  "Live Bytes in Heap": "堆中存活字节数",
  "Live Objects in Heap": "堆中存活对象数",
  "Memory classes": "内存分类",
  "MSpan/MCache": "MSpan / MCache 元数据",
  "Mutex wait time": "互斥锁等待时间",
  "Time Goroutines Spend in 'Runnable' state": "协程可运行状态等待时间",
  "Size Classes": "对象大小分布",
  "Stop-the-world Stopping Latencies (GC)": "全局暂停停止延迟（GC）",
  "Stop-the-world Stopping Latencies (Other)": "全局暂停停止延迟（其他）",
  "Stop-the-world Pause Latencies (Total)": "全局暂停总延迟（GC）",
  "Stop-the-world Pause Latencies (Other)": "全局暂停总延迟（其他）",
  "Threads": "线程",
  "calls": "调用次数",
  "calls from go to c": "Go 到 C 调用",
  "cpu-seconds / second": "CPU 秒 / 秒",
  "cpu-seconds per seconds": "CPU 秒 / 秒",
  "seconds / second": "秒 / 秒",
  "objects / second": "对象数 / 秒",
  "objects": "对象数",
  "bytes": "字节",
  "goroutines": "协程数",
  "threads": "线程数",
  "finalizers": "终结器数",
  "cleanups": "清理函数数",
  "cycles": "周期数",
  "user": "用户代码",
  "scavenge": "内存归还",
  "idle": "空闲",
  "gc total": "GC 总计",
  "total": "总计",
  "mark assist": "辅助标记",
  "mark dedicated": "专用标记",
  "mark idle": "空闲标记",
  "pause": "暂停",
  "assist": "辅助归还",
  "background": "后台归还",
  "memory limit": "内存上限",
  "in-use memory": "使用中内存",
  "heap live": "堆存活内存",
  "heap goal": "目标堆大小",
  "queue size": "队列长度",
  "automatic": "自动触发",
  "forced": "强制触发",
  "scannable globals": "可扫描全局变量",
  "scannable heap": "可扫描堆内存",
  "scanned stack": "已扫描栈内存",
  "created": "新建协程",
  "not in Go": "系统调用或 CGO 中",
  "runnable": "可运行",
  "running": "运行中",
  "waiting": "等待中",
  "new goroutines stack size": "新协程栈大小",
  "allocs/sec": "每秒分配",
  "frees/sec": "每秒释放",
  "heap sys": "堆向系统申请的内存",
  "heap objects": "堆对象内存",
  "heap stacks": "堆栈内存",
  "live bytes": "存活字节数",
  "live objects": "存活对象数",
  "os stacks": "系统线程栈",
  "other": "其他",
  "profiling buckets": "性能分析桶",
  "mspan in-use": "使用中的 mspan",
  "mspan free": "空闲 mspan",
  "mcache in-use": "使用中的 mcache",
  "mcache free": "空闲 mcache",
  "mutex wait": "互斥锁等待",
  "duration": "持续时间",
  "pause duration": "暂停时长",
  "stopping duration": "停止过程时长",
  "pauses": "暂停次数",
  "size class": "对象大小类别",
};

const cpuRate = "所有指标均为每秒消耗的 CPU 秒数。这些值是估计值，不能直接与系统 CPU 时间比较，只应与其他 /cpu/classes 指标比较。";
const stoppingInfo = (gc) => `此热力图显示<b>${gc ? "与 GC 相关" : "与 GC 无关"}</b>的全局暂停（stop-the-world）<i>停止延迟</i>分布。
停止延迟是从决定暂停程序到所有 P（逻辑处理器）均已停止所需的时间，是全局暂停总时长的一部分。在此期间，部分线程可能仍在执行。
使用 <b>/sched/pauses/stopping/${gc ? "gc" : "other"}:seconds</b>。`;
const totalPauseInfo = (gc) => `此热力图显示<b>${gc ? "与 GC 相关" : "与 GC 无关"}</b>的全局暂停（stop-the-world）<i>总延迟</i>分布。
总延迟是从决定暂停程序到程序重新开始运行的时间，其中包括等待所有线程停止的时间；在停止过程中，部分线程可能仍在运行。
停止过程的延迟由 <b>/sched/pauses/stopping/${gc ? "gc" : "other"}:seconds</b> 单独记录；总延迟使用 <b>/sched/pauses/total/${gc ? "gc" : "other"}:seconds</b>。`;

export const plotInfo = {
  "cgo": "显示当前进程每单位时间从 Go 调用 C 的次数。使用 <b>/cgo/go-to-c-calls:calls</b>。",
  "cpu-overall": `显示用户代码、运行时及空闲资源的 CPU 时间分布，用于了解整体利用率及剩余容量。累计指标会转换为每秒速率。
<i>用户代码</i>为 <b>/cpu/classes/user:cpu-seconds</b> 的每秒增量，即运行用户 Go 代码所消耗的 CPU 时间。
<i>内存归还</i>为 <b>/cpu/classes/scavenge:cpu-seconds</b> 的每秒增量，即将未使用内存归还操作系统所消耗的 CPU 时间。
<i>空闲</i>为 <b>/cpu/classes/idle:cpu-seconds</b> 的每秒增量，即未被用户代码或运行时使用的可用 CPU 时间。
<i>GC 总计</i>为 <b>/cpu/classes/gc/total:cpu-seconds</b> 的每秒增量，即所有 GC 任务消耗的 CPU 时间总和。
<i>总计</i>为 <b>/cpu/classes/total:cpu-seconds</b> 的每秒增量，即由 GOMAXPROCS 决定的可用 CPU 时间。
${cpuRate}`,
  "cpu-gc": `累计指标会转换为每秒速率，便于阅读和比较。
<i>辅助标记</i>为 <b>/cpu/classes/gc/mark/assist</b> 的每秒增量，即协程为避免 GC 落后于应用而协助执行 GC 任务所消耗的 CPU 时间。
<i>专用标记</i>为 <b>/cpu/classes/gc/mark/dedicated</b> 的每秒增量，即专用于 GC 的处理器所消耗的 CPU 时间。
<i>空闲标记</i>为 <b>/cpu/classes/gc/mark/idle</b> 的每秒增量，即使用调度器暂时无法分配给其他工作的空闲 CPU 资源执行 GC 的时间。
<i>暂停</i>为 <b>/cpu/classes/gc/pause</b> 的每秒增量，即应用因 GC 暂停期间的 CPU 时间。
${cpuRate}`,
  "cpu-scavenger": `显示 GC 内存归还器将内存归还操作系统时，主动归还与后台归还的 CPU 时间分布。
<i>辅助归还</i>为 <b>/cpu/classes/scavenge/assist</b> 的每秒增量，即在内存压力下主动归还未使用内存所消耗的 CPU 时间。
<i>后台归还</i>为 <b>/cpu/classes/scavenge/background</b> 的每秒增量，即后台归还未使用内存所消耗的 CPU 时间。
两项指标的单位均为 CPU 秒 / 秒。`,
  "garbage collection": `<i>内存上限</i>为 <b>/gc/gomemlimit:bytes</b>，由 GOMEMLIMIT 或 debug.SetMemoryLimit 配置的 Go 运行时内存限制，未配置时按服务提供的值显示。
<i>使用中内存</i>为全部已映射内存减去已释放的堆内存，即 <b>/memory/classes/total - /memory/classes/heap/released</b>。
<i>堆存活内存</i>为 <b>/gc/heap/live:bytes</b>，即存活对象占用的堆内存。
<i>目标堆大小</i>为 <b>/gc/heap/goal:bytes</b>，即每个 GC 周期结束时的目标堆大小。`,
  "gc-cleanups": `由 runtime.AddCleanup 创建的清理函数队列的近似长度。
计算方式为 <b>/gc/cleanups/queued:cleanups - /gc/cleanups/executed:cleanups</b>，可用于发现因清理函数执行缓慢导致的队列积压。`,
  "gc-finalizers": `通过 runtime.SetFinalizer 设置的终结器函数队列长度。
计算方式为 <b>/gc/finalizers/queued:finalizers - /gc/finalizers/executed:finalizers</b>，可用于发现因终结器过多或执行过慢导致的队列积压。`,
  "gc-cycles": "已完成的 GC 周期数量，包括强制触发及 Go 运行时自动触发的周期。",
  "gc-scan": `显示 GC 可以扫描的内存量。
<i>可扫描全局变量</i>为 <b>/gc/scan/globals</b>，即可扫描的全局变量空间总量。
<i>可扫描堆内存</i>为 <b>/gc/scan/heap</b>，即可扫描的堆空间总量。
<i>已扫描栈内存</i>为 <b>/gc/scan/stack</b>，即上一次 GC 周期扫描的栈字节数。`,
  "goroutines": `<i>协程数</i>为 <b>/sched/goroutines</b>，即当前存活的协程数量。
<i>新建协程</i>为累计指标 <b>/sched/goroutines-created</b> 的增量。
<i>系统调用或 CGO 中</i>为 <b>/sched/goroutines/not-in-go</b>，即在系统调用或 CGO 调用中运行或阻塞的近似协程数量。
<i>可运行</i>为 <b>/sched/goroutines/runnable</b>，即已就绪但尚未执行的近似协程数量。
<i>运行中</i>为 <b>/sched/goroutines/running</b>，即正在执行的近似协程数量。
<i>等待中</i>为 <b>/sched/goroutines/waiting</b>，即等待 I/O 或同步资源的近似协程数量。`,
  "gc-stack-size": "显示新协程的初始栈大小。使用 <b>/gc/stack/starting-size:bytes</b>。",
  "alloc-free-rate": `<i>每秒分配</i>为累计指标 <b>/gc/heap/allocs:objects</b> 的每秒增量。
<i>每秒释放</i>为累计指标 <b>/gc/heap/frees:objects</b> 的每秒增量。`,
  "heap (details)": `<i>堆向系统申请的内存</i>为 <b>/memory/classes/heap/{objects + unused + released + free}</b>，即从操作系统获取的全部堆内存的估计值。
<i>堆对象内存</i>为 <b>/memory/classes/heap/objects</b>，包括存活对象及已死亡但尚未被 GC 标记为空闲的对象。
<i>堆栈内存</i>为 <b>/memory/classes/heap/stacks</b>，即栈空间使用的内存。
<i>目标堆大小</i>为 <b>/gc/heap/goal</b>，即 GC 周期结束时的目标堆大小。`,
  "live-bytes": "<i>存活字节数</i>为 <b>/gc/heap/allocs - /gc/heap/frees</b>，即应用当前分配到堆上、尚未被垃圾回收的字节数。",
  "live-objects": "<i>存活对象数</i>为 <b>/gc/heap/objects</b>，即占用堆内存的存活对象及尚未清扫的对象数量。",
  "memory-classes": `<i>系统线程栈</i>为 <b>/memory/classes/os-stacks</b>，即底层操作系统分配的栈内存。
<i>其他</i>为 <b>/memory/classes/other</b>，包括执行跟踪缓冲区、运行时调试结构、终结器及性能分析相关结构等。
<i>性能分析桶</i>为 <b>/memory/classes/profiling/buckets</b>，即性能分析使用的栈跟踪哈希表占用的内存。
<i>总计</i>为 <b>/memory/classes/total</b>，即 Go 运行时映射到当前进程中的全部可读写内存。`,
  "mspan-mcache": `<i>使用中的 mspan</i>为 <b>/memory/classes/metadata/mspan/inuse</b>，即当前正在使用的运行时 mspan 结构占用的内存。
<i>空闲 mspan</i>为 <b>/memory/classes/metadata/mspan/free</b>，即已预留但尚未使用的 mspan 内存。
<i>使用中的 mcache</i>为 <b>/memory/classes/metadata/mcache/inuse</b>。
<i>空闲 mcache</i>为 <b>/memory/classes/metadata/mcache/free</b>，即已预留但尚未使用的 mcache 内存。`,
  "mutex-wait": `累计指标会转换为每秒速率，便于阅读和比较。
<i>互斥锁等待</i>为 <b>/sync/mutex/wait/total</b> 的每秒增量，表示协程因 sync.Mutex 或 sync.RWMutex 阻塞的近似累计时间。
此指标有助于发现锁竞争的整体变化。需要更详细的数据时，可使用 runtime/pprof 采集 mutex 或 block profile。`,
  "runnable-time": "此热力图显示协程在调度器中处于可运行状态、尚未真正开始执行的等待时间分布。使用 <b>/sched/latencies:seconds</b>。",
  "size-classes": "此热力图显示对象大小类别的分布，使用 <b>/gc/heap/allocs-by-size</b> 和 <b>/gc/heap/frees-by-size</b>。",
  "stopping-pauses-gc": stoppingInfo(true),
  "stopping-pauses-other": stoppingInfo(false),
  "total-pauses-gc": totalPauseInfo(true),
  "total-pauses-other": totalPauseInfo(false),
  "threads": "显示 Go 运行时当前拥有的存活线程数。使用 <b>/sched/threads/total:threads</b>。",
};

export const localizePlotConfig = (source, language = getLanguage()) => {
  const config = structuredClone(source);
  if (language !== "zh" || !Object.hasOwn(plotInfo, source.name)) return config;
  const label = (value) => plotLabels[value] || value;
  config.title = label(config.title);
  config.infoText = plotInfo[source.name];
  if (source.name === "goroutines" && source.subplots?.length === 1) {
    config.infoText = config.infoText.split("\n")[0];
  }
  if (config.layout?.yaxis) config.layout.yaxis.title = label(config.layout.yaxis.title);
  for (const subplot of config.subplots || []) subplot.name = label(subplot.name);
  if (config.hover) {
    config.hover.yname = label(config.hover.yname);
    config.hover.zname = label(config.hover.zname);
  }
  return config;
};
