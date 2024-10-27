<template>
<div class="content" :style='{"minHeight":"100vh","padding":"0px","background":"url(http://codegen.caihongy.cn/20230414/c294e7815ede4f498ef5df52f1fb482c.png) no-repeat center top / 100% 100%"}'>
	<div class="text" :style='{"margin":"50px auto","fontSize":"24px","color":"rgb(51, 51, 51)","textAlign":"center","fontWeight":"bold"}'>欢迎使用 {{this.$project.projectName}}</div>
    <div class="cardView">
        <div class="cards" :style='{"margin":"0 0 20px 0","alignItems":"center","justifyContent":"center","display":"flex"}'>
			<div :style='{"boxShadow":"0 1px 6px rgba(0,0,0,.3)","margin":"0 10px","borderRadius":"40px","display":"flex"}' v-if="isAuth('yonghu','首页总数')">
				<div :style='{"width":"80px","borderRadius":"40px","background":"#1ba7a6","height":"80px"}'></div>
				<div :style='{"width":"160px","alignItems":"center","flexDirection":"column","justifyContent":"center","display":"flex"}'>
					<div :style='{"margin":"5px 0","lineHeight":"24px","fontSize":"20px","color":"#333","fontWeight":"bold","height":"24px"}'>{{yonghuCount}}</div>
					<div :style='{"margin":"5px 0","lineHeight":"24px","fontSize":"16px","color":"#666","height":"24px"}'>用户总数</div>
				</div>
			</div>
			<div :style='{"boxShadow":"0 1px 6px rgba(0,0,0,.3)","margin":"0 10px","borderRadius":"40px","display":"flex"}' v-if="isAuth('wupinxinxi','首页总数')">
				<div :style='{"width":"80px","borderRadius":"40px","background":"#1ba7a6","height":"80px"}'></div>
				<div :style='{"width":"160px","alignItems":"center","flexDirection":"column","justifyContent":"center","display":"flex"}'>
					<div :style='{"margin":"5px 0","lineHeight":"24px","fontSize":"20px","color":"#333","fontWeight":"bold","height":"24px"}'>{{wupinxinxiCount}}</div>
					<div :style='{"margin":"5px 0","lineHeight":"24px","fontSize":"16px","color":"#666","height":"24px"}'>物品信息总数</div>
				</div>
			</div>
			<div :style='{"boxShadow":"0 1px 6px rgba(0,0,0,.3)","margin":"0 10px","borderRadius":"40px","display":"flex"}' v-if="isAuth('wupinzulin','首页总数')">
				<div :style='{"width":"80px","borderRadius":"40px","background":"#1ba7a6","height":"80px"}'></div>
				<div :style='{"width":"160px","alignItems":"center","flexDirection":"column","justifyContent":"center","display":"flex"}'>
					<div :style='{"margin":"5px 0","lineHeight":"24px","fontSize":"20px","color":"#333","fontWeight":"bold","height":"24px"}'>{{wupinzulinCount}}</div>
					<div :style='{"margin":"5px 0","lineHeight":"24px","fontSize":"16px","color":"#666","height":"24px"}'>物品租赁总数</div>
				</div>
			</div>
			<div :style='{"boxShadow":"0 1px 6px rgba(0,0,0,.3)","margin":"0 10px","borderRadius":"40px","display":"flex"}' v-if="isAuth('wupinchuzu','首页总数')">
				<div :style='{"width":"80px","borderRadius":"40px","background":"#1ba7a6","height":"80px"}'></div>
				<div :style='{"width":"160px","alignItems":"center","flexDirection":"column","justifyContent":"center","display":"flex"}'>
					<div :style='{"margin":"5px 0","lineHeight":"24px","fontSize":"20px","color":"#333","fontWeight":"bold","height":"24px"}'>{{wupinchuzuCount}}</div>
					<div :style='{"margin":"5px 0","lineHeight":"24px","fontSize":"16px","color":"#666","height":"24px"}'>物品出租总数</div>
				</div>
			</div>
        </div>
        <div style="display: flex;align-items: center;width: 100%;margin-bottom: 10px;">
            <el-card style="width: ;margin: 0 10px;" v-if="isAuth('yonghu','首页统计')">
                <div id="yonghuChart1" style="width:100%;height:400px;"></div>
            </el-card>
            <el-card style="width: ;margin: 0 10px;" v-if="isAuth('wupinxinxi','首页统计')">
                <div id="wupinxinxiChart1" style="width:100%;height:400px;"></div>
            </el-card>
            <el-card style="width: ;margin: 0 10px;" v-if="isAuth('wupinzulin','首页统计')">
                <div id="wupinzulinChart1" style="width:100%;height:400px;"></div>
            </el-card>
            <el-card style="width: ;margin: 0 10px;" v-if="isAuth('wupinchuzu','首页统计')">
                <div id="wupinchuzuChart1" style="width:100%;height:400px;"></div>
            </el-card>
            <el-card style="width: ;margin: 0 10px;" v-if="isAuth('wupinchuzu','首页统计')">
                <div id="wupinchuzuChart2" style="width:100%;height:400px;"></div>
            </el-card>
            <el-card style="width: ;margin: 0 10px;" v-if="isAuth('wupinchuzu','首页统计')">
                <div id="wupinchuzuChart3" style="width:100%;height:400px;"></div>
            </el-card>
            <el-card style="width: ;margin: 0 10px;" v-if="isAuth('wupinchuzu','首页统计')">
                <div id="wupinchuzuChart4" style="width:100%;height:400px;"></div>
            </el-card>
            <el-card style="width: ;margin: 0 10px;" v-if="isAuth('wupinchuzu','首页统计')">
                <div id="wupinchuzuChart5" style="width:100%;height:400px;"></div>
            </el-card>
        </div>
    </div>
</div>
</template>
<script>
//8
import router from '@/router/router-static'
import * as echarts from 'echarts'
export default {
	data() {
		return {
            yonghuCount: 0,
            wupinxinxiCount: 0,
            wupinzulinCount: 0,
            wupinchuzuCount: 0,
		};
	},
  mounted(){
    this.init();
    this.getyonghuCount();
    this.yonghuChat1();
    this.getwupinxinxiCount();
    this.wupinxinxiChat1();
    this.getwupinzulinCount();
    this.wupinzulinChat1();
    this.getwupinchuzuCount();
    this.wupinchuzuChat1();
    this.wupinchuzuChat2();
    this.wupinchuzuChat3();
    this.wupinchuzuChat4();
    this.wupinchuzuChat5();
  },
  methods:{
    init(){
        if(this.$storage.get('Token')){
        this.$http({
            url: `${this.$storage.get('sessionTable')}/session`,
            method: "get"
        }).then(({ data }) => {
            if (data && data.code != 0) {
            router.push({ name: 'login' })
            }
        });
        }else{
            router.push({ name: 'login' })
        }
    },
    getyonghuCount() {
        this.$http({
            url: `yonghu/count`,
            method: "get"
        }).then(({
            data
        }) => {
            if (data && data.code == 0) {
                this.yonghuCount = data.data
            }
        })
    },

    yonghuChat1() {
      this.$nextTick(()=>{

        var yonghuChart1 = echarts.init(document.getElementById("yonghuChart1"),'macarons');
        this.$http({
            url: "yonghu/group/xingbie",
            method: "get",
        }).then(({ data }) => {
            if (data && data.code === 0) {
                let res = data.data;
                let xAxis = [];
                let yAxis = [];
                let pArray = []
                for(let i=0;i<res.length;i++){
                    xAxis.push(res[i].xingbie);
                    yAxis.push(parseFloat((res[i].total)));
                    pArray.push({
                        value: parseFloat((res[i].total)),
                        name: res[i].xingbie
                    })
                }
                var option = {};
                option = {
                        title: {
                            text: '用户量统计',
                            left: 'center'
                        },
                        tooltip: {
                          trigger: 'item',
                          formatter: '{b} : {c} ({d}%)'
                        },
                        series: [
                            {
                                type: 'pie',
                                radius: ['25%', '55%'],
                                roseType: 'area',
                                center: ['50%', '60%'],
                                data: pArray,
                                emphasis: {
                                    itemStyle: {
                                        shadowBlur: 10,
                                        shadowOffsetX: 0,
                                        shadowColor: 'rgba(0, 0, 0, 0.5)'
                                    }
                                }
                            }
                        ]
                };
                // 使用刚指定的配置项和数据显示图表。
                yonghuChart1.setOption(option);
                  //根据窗口的大小变动图表
                window.onresize = function() {
                    yonghuChart1.resize();
                };
            }
        });
      })
    },






    getwupinxinxiCount() {
        this.$http({
            url: `wupinxinxi/count`,
            method: "get"
        }).then(({
            data
        }) => {
            if (data && data.code == 0) {
                this.wupinxinxiCount = data.data
            }
        })
    },

    wupinxinxiChat1() {
      this.$nextTick(()=>{

        var wupinxinxiChart1 = echarts.init(document.getElementById("wupinxinxiChart1"),'macarons');
        this.$http({
            url: `wupinxinxi/value/fabushijian/wupinshuliang/月`,
            method: "get",
        }).then(({ data }) => {
            if (data && data.code === 0) {
                let res = data.data;
                let xAxis = [];
                let yAxis = [];
                let pArray = []
                for(let i=0;i<res.length;i++){
                    xAxis.push(res[i].fabushijian);
                    yAxis.push(parseFloat((res[i].total)));
                    pArray.push({
                        value: parseFloat((res[i].total)),
                        name: res[i].fabushijian
                    })
                }
                var option = {};
                option = {
                    title: {
                        text: '出租量统计',
                        left: 'center'
                    },
                    tooltip: {
                      trigger: 'item',
                      formatter: '{b} : {c}'
                    },
                    xAxis: {
                        type: 'category',
                        data: xAxis,
                        axisLabel : {
                            rotate:70
                        }
                    },
                    yAxis: {
                        type: 'value'
                    },
                    series: [{
                        data: yAxis,
                        type: 'bar'
                    }]
                };
                // 使用刚指定的配置项和数据显示图表。
                wupinxinxiChart1.setOption(option);
                  //根据窗口的大小变动图表
                window.onresize = function() {
                    wupinxinxiChart1.resize();
                };
            }
        });
      })
    },






    getwupinzulinCount() {
        this.$http({
            url: `wupinzulin/count`,
            method: "get"
        }).then(({
            data
        }) => {
            if (data && data.code == 0) {
                this.wupinzulinCount = data.data
            }
        })
    },

    wupinzulinChat1() {
      this.$nextTick(()=>{

        var wupinzulinChart1 = echarts.init(document.getElementById("wupinzulinChart1"),'macarons');
        this.$http({
            url: `wupinzulin/value/zulinshijian/wupinshuliang/月`,
            method: "get",
        }).then(({ data }) => {
            if (data && data.code === 0) {
                let res = data.data;
                let xAxis = [];
                let yAxis = [];
                let pArray = []
                for(let i=0;i<res.length;i++){
                    xAxis.push(res[i].zulinshijian);
                    yAxis.push(parseFloat((res[i].total)));
                    pArray.push({
                        value: parseFloat((res[i].total)),
                        name: res[i].zulinshijian
                    })
                }
                var option = {};
                option = {
                    title: {
                        text: '租赁数量统计',
                        left: 'center'
                    },
                    tooltip: {
                      trigger: 'item',
                      formatter: '{b} : {c}'
                    },
                    xAxis: {
                        type: 'category',
                        boundaryGap: false,
                        data: xAxis
                    },
                    yAxis: {
                        type: 'value'
                    },
                    series: [{
                        data: yAxis,
                        type: 'line',
                    }]
                };
                // 使用刚指定的配置项和数据显示图表。
                wupinzulinChart1.setOption(option);
                  //根据窗口的大小变动图表
                window.onresize = function() {
                    wupinzulinChart1.resize();
                };
            }
        });
      })
    },






    getwupinchuzuCount() {
        this.$http({
            url: `wupinchuzu/count`,
            method: "get"
        }).then(({
            data
        }) => {
            if (data && data.code == 0) {
                this.wupinchuzuCount = data.data
            }
        })
    },

    wupinchuzuChat1() {
      this.$nextTick(()=>{

        var wupinchuzuChart1 = echarts.init(document.getElementById("wupinchuzuChart1"),'macarons');
        this.$http({
            url: "wupinchuzu/group/leibie",
            method: "get",
        }).then(({ data }) => {
            if (data && data.code === 0) {
                let res = data.data;
                let xAxis = [];
                let yAxis = [];
                let pArray = []
                for(let i=0;i<res.length;i++){
                    xAxis.push(res[i].leibie);
                    yAxis.push(parseFloat((res[i].total)));
                    pArray.push({
                        value: parseFloat((res[i].total)),
                        name: res[i].leibie
                    })
                }
                var option = {};
                option = {
                        title: {
                            text: '类别统计',
                            left: 'center'
                        },
                        tooltip: {
                          trigger: 'item',
                          formatter: '{b} : {c} ({d}%)'
                        },
                        series: [
                            {
                                type: 'pie',
                                radius: '55%',
                                center: ['50%', '60%'],
                                data: pArray,
                                emphasis: {
                                    itemStyle: {
                                        shadowBlur: 10,
                                        shadowOffsetX: 0,
                                        shadowColor: 'rgba(0, 0, 0, 0.5)'
                                    }
                                }
                            }
                        ]
                };
                // 使用刚指定的配置项和数据显示图表。
                wupinchuzuChart1.setOption(option);
                  //根据窗口的大小变动图表
                window.onresize = function() {
                    wupinchuzuChart1.resize();
                };
            }
        });
      })
    },

    wupinchuzuChat2() {
      this.$nextTick(()=>{

        var wupinchuzuChart2 = echarts.init(document.getElementById("wupinchuzuChart2"),'macarons');
        this.$http({
            url: "wupinchuzu/group/fuwuquyu",
            method: "get",
        }).then(({ data }) => {
            if (data && data.code === 0) {
                let res = data.data;
                let xAxis = [];
                let yAxis = [];
                let pArray = []
                for(let i=0;i<res.length;i++){
                    xAxis.push(res[i].fuwuquyu);
                    yAxis.push(parseFloat((res[i].total)));
                    pArray.push({
                        value: parseFloat((res[i].total)),
                        name: res[i].fuwuquyu
                    })
                }
                var option = {};
                // 使用刚指定的配置项和数据显示图表。
                wupinchuzuChart2.setOption(option);
                  //根据窗口的大小变动图表
                window.onresize = function() {
                    wupinchuzuChart2.resize();
                };
            }
        });
      })
    },

    wupinchuzuChat3() {
      this.$nextTick(()=>{

        var wupinchuzuChart3 = echarts.init(document.getElementById("wupinchuzuChart3"),'macarons');
        this.$http({
            url: "wupinchuzu/group/lianxiren",
            method: "get",
        }).then(({ data }) => {
            if (data && data.code === 0) {
                let res = data.data;
                let xAxis = [];
                let yAxis = [];
                let pArray = []
                for(let i=0;i<res.length;i++){
                    xAxis.push(res[i].lianxiren);
                    yAxis.push(parseFloat((res[i].total)));
                    pArray.push({
                        value: parseFloat((res[i].total)),
                        name: res[i].lianxiren
                    })
                }
                var option = {};
                option = {
                        title: {
                            text: '联系人统计',
                            left: 'center'
                        },
                        tooltip: {
                          trigger: 'item',
                          formatter: '{b} : {c} ({d}%)'
                        },
                        series: [
                            {
                                type: 'pie',
                                radius: ['25%', '55%'],
                                center: ['50%', '60%'],
                                data: pArray,
                                emphasis: {
                                    itemStyle: {
                                        shadowBlur: 10,
                                        shadowOffsetX: 0,
                                        shadowColor: 'rgba(0, 0, 0, 0.5)'
                                    }
                                }
                            }
                        ]
                };
                // 使用刚指定的配置项和数据显示图表。
                wupinchuzuChart3.setOption(option);
                  //根据窗口的大小变动图表
                window.onresize = function() {
                    wupinchuzuChart3.resize();
                };
            }
        });
      })
    },


    wupinchuzuChat4() {
      this.$nextTick(()=>{

        var wupinchuzuChart4 = echarts.init(document.getElementById("wupinchuzuChart4"),'macarons');
        this.$http({
            url: "wupinchuzu/group/sjdz",
            method: "get",
        }).then(({ data }) => {
            if (data && data.code === 0) {
                let res = data.data;
                let xAxis = [];
                let yAxis = [];
                let pArray = []
                for(let i=0;i<res.length;i++){
                    xAxis.push(res[i].sjdz);
                    yAxis.push(parseFloat((res[i].total)));
                    pArray.push({
                        value: parseFloat((res[i].total)),
                        name: res[i].sjdz
                    })
                }
                var option = {};
                option = {
                    title: {
                        text: '商家地址统计',
                        left: 'center'
                    },
                    tooltip: {
                      trigger: 'item',
                      formatter: '{b} : {c}'
                    },
                    xAxis: {
                        type: 'category',
                        data: xAxis,
                        axisLabel : {
                            rotate:70
                        }
                    },
                    yAxis: {
                        type: 'value'
                    },
                    series: [{
                        data: yAxis,
                        type: 'bar'
                    }]
                };
                // 使用刚指定的配置项和数据显示图表。
                wupinchuzuChart4.setOption(option);
                  //根据窗口的大小变动图表
                window.onresize = function() {
                    wupinchuzuChart4.resize();
                };
            }
        });
      })
    },

    wupinchuzuChat5() {
      this.$nextTick(()=>{

        var wupinchuzuChart5 = echarts.init(document.getElementById("wupinchuzuChart5"),'macarons');
        this.$http({
            url: "wupinchuzu/group/fatie",
            method: "get",
        }).then(({ data }) => {
            if (data && data.code === 0) {
                let res = data.data;
                let xAxis = [];
                let yAxis = [];
                let pArray = []
                for(let i=0;i<res.length;i++){
                    xAxis.push(res[i].fatie);
                    yAxis.push(parseFloat((res[i].total)));
                    pArray.push({
                        value: parseFloat((res[i].total)),
                        name: res[i].fatie
                    })
                }
                var option = {};
                option = {
                    title: {
                        text: '发帖统计',
                        left: 'center'
                    },
                    tooltip: {
                      trigger: 'item',
                      formatter: '{b} : {c}'
                    },
                    xAxis: {
                        type: 'category',
                        boundaryGap: false,
                        data: xAxis
                    },
                    yAxis: {
                        type: 'value'
                    },
                    series: [{
                        data: yAxis,
                        type: 'line',
                        areaStyle: {},
                        smooth: true
                    }]
                };
                // 使用刚指定的配置项和数据显示图表。
                wupinchuzuChart5.setOption(option);
                  //根据窗口的大小变动图表
                window.onresize = function() {
                    wupinchuzuChart5.resize();
                };
            }
        });
      })
    },

  }
};
</script>
<style lang="scss" scoped>
    .cardView {
        display: flex;
        flex-wrap: wrap;
        width: 100%;

        .cards {
            display: flex;
            align-items: center;
            width: 100%;
            margin-bottom: 10px;
            justify-content: center;
            .card {
                width: calc(25% - 20px);
                margin: 0 10px;
                /deep/.el-card__body{
                    padding: 0;
                }
            }
        }
    }
</style>
