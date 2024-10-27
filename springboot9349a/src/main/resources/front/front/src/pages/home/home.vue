<template>
<div class="home-preview" :style='{"width":"100%","margin":"10px auto"}'>



<div class="recommend" :style='{"padding":"10px 0","margin":"10px 0 10px","backgroundSize":"100% 100%","backgroundImage":"url(http://codegen.caihongy.cn/20220914/11e43848ddb044afa556a24b7d7ceb58.png)","height":"1170px"}'>
	<div v-if="false" class="idea recommendIdea" :style='{"padding":"20px","flexWrap":"wrap","background":"#efefef","justifyContent":"space-between","display":"flex"}'>
		<div class="box1" :style='{"width":"20%","background":"#fff","height":"80px"}'></div>
		<div class="box2" :style='{"width":"20%","background":"#fff","height":"80px"}'></div>
		<div class="box3" :style='{"width":"20%","background":"#fff","height":"80px"}'></div>
		<div class="box4" :style='{"width":"20%","background":"#fff","height":"80px"}'></div>
		<div class="box5" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box6" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box7" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box8" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box9" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box10" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
	</div>
	
    <div class="title" :style='{"width":"auto","margin":"10px auto","lineHeight":"54px","textAlign":"center"}'>
		<span :style='{"color":"#000","fontSize":"30px","fontWeight":"600"}'>物品信息推荐</span>
	</div>
	
	
	
	
	
	
	
	
	<!-- 样式七 -->
	<div class="list list7 index-pv1" :style='{"width":"100%","padding":"10px 460px","position":"relative","display":"flex","height":"1000px"}'>
		<div v-if="wupinxinxiRecommend.length>0" class="list-item animation-box" @click="toDetail('wupinxinxiDetail', wupinxinxiRecommend[0])" :style='{"boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","margin":"0 10px","overflow":"hidden","borderRadius":"15px","background":"#fff","width":"400px","position":"relative","height":"480px"}'>
			<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"384px"}' v-if="preHttp(wupinxinxiRecommend[0].wupintupian)" :src="wupinxinxiRecommend[0].wupintupian.split(',')[0]" alt="" />
			<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"384px"}' v-else :src="baseUrl + (wupinxinxiRecommend[0].wupintupian?wupinxinxiRecommend[0].wupintupian.split(',')[0]:'')" alt="" />
			<div class="title line1" :style='{"overflow":"hidden","color":"#000","textAlign":"center","width":"100%","lineHeight":"90px","fontSize":"18px","height":"20%"}'>
                <div>物品名称:{{wupinxinxiRecommend[0].wupinmingcheng}}</div>
                <div>品牌:{{wupinxinxiRecommend[0].pinpai}}</div>
                <div>物品类别:{{wupinxinxiRecommend[0].wupinleibie}}</div>
            </div>
		</div>
		<div :style='{"width":"600px","position":"absolute","top":"510px","display":"flex","height":"250px"}' class="list-body">
			<div v-if="wupinxinxiRecommend.length>1" @click="toDetail('wupinxinxiDetail', wupinxinxiRecommend[1])" class="list-item animation-box" :style='{"boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","margin":"0 10px","overflow":"hidden","borderRadius":"15px","background":"#fff","width":"275px","position":"relative","height":"275px"}'>
				<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"75%"}' v-if="preHttp(wupinxinxiRecommend[1].wupintupian)" :src="wupinxinxiRecommend[1].wupintupian.split(',')[0]" alt="" />
				<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"75%"}' v-else :src="baseUrl + (wupinxinxiRecommend[1].wupintupian?wupinxinxiRecommend[1].wupintupian.split(',')[0]:'')" alt="" />
				<div class="title line1" :style='{"overflow":"hidden","color":"#000","textAlign":"center","width":"100%","lineHeight":"60px","fontSize":"18px","height":"25%"}'>
                    <div>物品名称:{{wupinxinxiRecommend[1].wupinmingcheng}}</div>
                    <div>品牌:{{wupinxinxiRecommend[1].pinpai}}</div>
                    <div>物品类别:{{wupinxinxiRecommend[1].wupinleibie}}</div>
                </div>
			</div>
			<div v-if="wupinxinxiRecommend.length>2" @click="toDetail('wupinxinxiDetail', wupinxinxiRecommend[2])" class="list-item animation-box" :style='{"boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","margin":"0 0 0 25px","overflow":"hidden","borderRadius":"15px","background":"#fff","width":"275px","position":"relative","height":"275px"}'>
				<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"75%"}' v-if="preHttp(wupinxinxiRecommend[2].wupintupian)" :src="wupinxinxiRecommend[2].wupintupian.split(',')[0]" alt="" />
				<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"75%"}' v-else :src="baseUrl +  (wupinxinxiRecommend[2].wupintupian?wupinxinxiRecommend[2].wupintupian.split(',')[0]:'')" alt="" />
				<div class="list-4-item-title line1" :style='{"overflow":"hidden","color":"#000","textAlign":"center","width":"100%","lineHeight":"60px","fontSize":"18px","height":"25%"}'>
                    <div>物品名称:{{wupinxinxiRecommend[2].wupinmingcheng}}</div>
                    <div>品牌:{{wupinxinxiRecommend[2].pinpai}}</div>
                    <div>物品类别:{{wupinxinxiRecommend[2].wupinleibie}}</div>
                </div>
			</div>
		</div>
		<div v-if="wupinxinxiRecommend.length>3" class="list-item animation-box" @click="toDetail('wupinxinxiDetail', wupinxinxiRecommend[3])" :style='{"boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","margin":"0 80px","overflow":"hidden","borderRadius":"15px","background":"#fff","width":"400px","position":"relative","height":"480px"}'>
			<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"80%"}' v-if="preHttp(wupinxinxiRecommend[3].wupintupian)" :src="wupinxinxiRecommend[3].wupintupian.split(',')[0]" alt="" />
			<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"80%"}' v-else :src="baseUrl + (wupinxinxiRecommend[3].wupintupian?wupinxinxiRecommend[3].wupintupian.split(',')[0]:'')" alt="" />
			<div class="list-4-item-title line1" :style='{"overflow":"hidden","color":"#000","textAlign":"center","width":"100%","lineHeight":"90px","fontSize":"18px","height":"20%"}'>
                <div>物品名称:{{wupinxinxiRecommend[3].wupinmingcheng}}</div>
                <div>品牌:{{wupinxinxiRecommend[3].pinpai}}</div>
                <div>物品类别:{{wupinxinxiRecommend[3].wupinleibie}}</div>
            </div>
		</div>
		<div :style='{"margin":"0 10px","top":"510px","left":"1075px","flexWrap":"wrap","display":"flex","width":"275px","position":"absolute","height":"275px"}' class="list-body">
			<div v-if="wupinxinxiRecommend.length>4" @click="toDetail('wupinxinxiDetail', wupinxinxiRecommend[4])" class="list-item animation-box" :style='{"boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","margin":"0 0 20px","overflow":"hidden","borderRadius":"15px","width":"275px","position":"relative","height":"275px"}'>
				<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"75%"}' v-if="preHttp(wupinxinxiRecommend[4].wupintupian)" :src="wupinxinxiRecommend[4].wupintupian.split(',')[0]" alt="" />
				<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"75%"}' v-else :src="baseUrl + (wupinxinxiRecommend[4].wupintupian?wupinxinxiRecommend[4].wupintupian.split(',')[0]:'')" alt="" />
				<div class="title line1" :style='{"overflow":"hidden","color":"#000","textAlign":"center","width":"100%","lineHeight":"60px","fontSize":"18px","height":"25%"}'>
                    <div>物品名称:{{wupinxinxiRecommend[4].wupinmingcheng}}</div>
                    <div>品牌:{{wupinxinxiRecommend[4].pinpai}}</div>
                    <div>物品类别:{{wupinxinxiRecommend[4].wupinleibie}}</div>
                </div>
			</div>
			<div v-if="wupinxinxiRecommend.length>5" @click="toDetail('wupinxinxiDetail', wupinxinxiRecommend[5])" class="list-item animation-box" :style='{"boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","padding":"15px","margin":"20px 0 0","overflow":"hidden","top":"270px","borderRadius":"15px","left":"-615px","width":"890px","position":"absolute","height":"205px"}'>
				<img :style='{"width":"40%","objectFit":"cover","borderRadius":"15px","float":"left","height":"100%"}' v-if="preHttp(wupinxinxiRecommend[5].wupintupian)" :src="wupinxinxiRecommend[5].wupintupian.split(',')[0]" alt="" />
				<img :style='{"width":"40%","objectFit":"cover","borderRadius":"15px","float":"left","height":"100%"}' v-else :src="baseUrl + (wupinxinxiRecommend[5].wupintupian?wupinxinxiRecommend[5].wupintupian.split(',')[0]:'')" alt="" />
				<div class="title line1" :style='{"overflow":"hidden","color":"#000","textAlign":"center","width":"55%","lineHeight":"180px","fontSize":"18px","height":"180px"}'>
                    <div>物品名称:{{wupinxinxiRecommend[5].wupinmingcheng}}</div>
                    <div>品牌:{{wupinxinxiRecommend[5].pinpai}}</div>
                    <div>物品类别:{{wupinxinxiRecommend[5].wupinleibie}}</div>
                </div>
			</div>
		</div>
	</div>
	
	
	
	<div @click="moreBtn('wupinxinxi')" :style='{"border":"0","boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","margin":"30px auto","borderRadius":"10px","textAlign":"center","display":"block","width":"150px","lineHeight":"32px"}'>
		<span :style='{"color":"#000","fontSize":"14px"}'>查看更多</span>
		<i v-if="true" :style='{"color":"#000","fontSize":"14px"}' class="el-icon-d-arrow-right"></i>
	</div>
	
</div>
<div class="recommend" :style='{"padding":"10px 0","margin":"10px 0 10px","backgroundSize":"100% 100%","backgroundImage":"url(http://codegen.caihongy.cn/20220914/11e43848ddb044afa556a24b7d7ceb58.png)","height":"1170px"}'>
	<div v-if="false" class="idea recommendIdea" :style='{"padding":"20px","flexWrap":"wrap","background":"#efefef","justifyContent":"space-between","display":"flex"}'>
		<div class="box1" :style='{"width":"20%","background":"#fff","height":"80px"}'></div>
		<div class="box2" :style='{"width":"20%","background":"#fff","height":"80px"}'></div>
		<div class="box3" :style='{"width":"20%","background":"#fff","height":"80px"}'></div>
		<div class="box4" :style='{"width":"20%","background":"#fff","height":"80px"}'></div>
		<div class="box5" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box6" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box7" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box8" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box9" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box10" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
	</div>
	
    <div class="title" :style='{"width":"auto","margin":"10px auto","lineHeight":"54px","textAlign":"center"}'>
		<span :style='{"color":"#000","fontSize":"30px","fontWeight":"600"}'>物品出租推荐</span>
	</div>
	
	
	
	
	
	
	
	
	<!-- 样式七 -->
	<div class="list list7 index-pv1" :style='{"width":"100%","padding":"10px 460px","position":"relative","display":"flex","height":"1000px"}'>
		<div v-if="wupinchuzuRecommend.length>0" class="list-item animation-box" @click="toDetail('wupinchuzuDetail', wupinchuzuRecommend[0])" :style='{"boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","margin":"0 10px","overflow":"hidden","borderRadius":"15px","background":"#fff","width":"400px","position":"relative","height":"480px"}'>
			<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"384px"}' v-if="preHttp(wupinchuzuRecommend[0].fengmian)" :src="wupinchuzuRecommend[0].fengmian.split(',')[0]" alt="" />
			<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"384px"}' v-else :src="baseUrl + (wupinchuzuRecommend[0].fengmian?wupinchuzuRecommend[0].fengmian.split(',')[0]:'')" alt="" />
			<div class="title line1" :style='{"overflow":"hidden","color":"#000","textAlign":"center","width":"100%","lineHeight":"90px","fontSize":"18px","height":"20%"}'>
                <div>类别:{{wupinchuzuRecommend[0].leibie}}</div>
                <div>联系人:{{wupinchuzuRecommend[0].lianxiren}}</div>
            </div>
		</div>
		<div :style='{"width":"600px","position":"absolute","top":"510px","display":"flex","height":"250px"}' class="list-body">
			<div v-if="wupinchuzuRecommend.length>1" @click="toDetail('wupinchuzuDetail', wupinchuzuRecommend[1])" class="list-item animation-box" :style='{"boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","margin":"0 10px","overflow":"hidden","borderRadius":"15px","background":"#fff","width":"275px","position":"relative","height":"275px"}'>
				<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"75%"}' v-if="preHttp(wupinchuzuRecommend[1].fengmian)" :src="wupinchuzuRecommend[1].fengmian.split(',')[0]" alt="" />
				<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"75%"}' v-else :src="baseUrl + (wupinchuzuRecommend[1].fengmian?wupinchuzuRecommend[1].fengmian.split(',')[0]:'')" alt="" />
				<div class="title line1" :style='{"overflow":"hidden","color":"#000","textAlign":"center","width":"100%","lineHeight":"60px","fontSize":"18px","height":"25%"}'>
                    <div>类别:{{wupinchuzuRecommend[1].leibie}}</div>
                    <div>联系人:{{wupinchuzuRecommend[1].lianxiren}}</div>
                </div>
			</div>
			<div v-if="wupinchuzuRecommend.length>2" @click="toDetail('wupinchuzuDetail', wupinchuzuRecommend[2])" class="list-item animation-box" :style='{"boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","margin":"0 0 0 25px","overflow":"hidden","borderRadius":"15px","background":"#fff","width":"275px","position":"relative","height":"275px"}'>
				<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"75%"}' v-if="preHttp(wupinchuzuRecommend[2].fengmian)" :src="wupinchuzuRecommend[2].fengmian.split(',')[0]" alt="" />
				<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"75%"}' v-else :src="baseUrl +  (wupinchuzuRecommend[2].fengmian?wupinchuzuRecommend[2].fengmian.split(',')[0]:'')" alt="" />
				<div class="list-4-item-title line1" :style='{"overflow":"hidden","color":"#000","textAlign":"center","width":"100%","lineHeight":"60px","fontSize":"18px","height":"25%"}'>
                    <div>类别:{{wupinchuzuRecommend[2].leibie}}</div>
                    <div>联系人:{{wupinchuzuRecommend[2].lianxiren}}</div>
                </div>
			</div>
		</div>
		<div v-if="wupinchuzuRecommend.length>3" class="list-item animation-box" @click="toDetail('wupinchuzuDetail', wupinchuzuRecommend[3])" :style='{"boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","margin":"0 80px","overflow":"hidden","borderRadius":"15px","background":"#fff","width":"400px","position":"relative","height":"480px"}'>
			<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"80%"}' v-if="preHttp(wupinchuzuRecommend[3].fengmian)" :src="wupinchuzuRecommend[3].fengmian.split(',')[0]" alt="" />
			<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"80%"}' v-else :src="baseUrl + (wupinchuzuRecommend[3].fengmian?wupinchuzuRecommend[3].fengmian.split(',')[0]:'')" alt="" />
			<div class="list-4-item-title line1" :style='{"overflow":"hidden","color":"#000","textAlign":"center","width":"100%","lineHeight":"90px","fontSize":"18px","height":"20%"}'>
                <div>类别:{{wupinchuzuRecommend[3].leibie}}</div>
                <div>联系人:{{wupinchuzuRecommend[3].lianxiren}}</div>
            </div>
		</div>
		<div :style='{"margin":"0 10px","top":"510px","left":"1075px","flexWrap":"wrap","display":"flex","width":"275px","position":"absolute","height":"275px"}' class="list-body">
			<div v-if="wupinchuzuRecommend.length>4" @click="toDetail('wupinchuzuDetail', wupinchuzuRecommend[4])" class="list-item animation-box" :style='{"boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","margin":"0 0 20px","overflow":"hidden","borderRadius":"15px","width":"275px","position":"relative","height":"275px"}'>
				<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"75%"}' v-if="preHttp(wupinchuzuRecommend[4].fengmian)" :src="wupinchuzuRecommend[4].fengmian.split(',')[0]" alt="" />
				<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"75%"}' v-else :src="baseUrl + (wupinchuzuRecommend[4].fengmian?wupinchuzuRecommend[4].fengmian.split(',')[0]:'')" alt="" />
				<div class="title line1" :style='{"overflow":"hidden","color":"#000","textAlign":"center","width":"100%","lineHeight":"60px","fontSize":"18px","height":"25%"}'>
                    <div>类别:{{wupinchuzuRecommend[4].leibie}}</div>
                    <div>联系人:{{wupinchuzuRecommend[4].lianxiren}}</div>
                </div>
			</div>
			<div v-if="wupinchuzuRecommend.length>5" @click="toDetail('wupinchuzuDetail', wupinchuzuRecommend[5])" class="list-item animation-box" :style='{"boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","padding":"15px","margin":"20px 0 0","overflow":"hidden","top":"270px","borderRadius":"15px","left":"-615px","width":"890px","position":"absolute","height":"205px"}'>
				<img :style='{"width":"40%","objectFit":"cover","borderRadius":"15px","float":"left","height":"100%"}' v-if="preHttp(wupinchuzuRecommend[5].fengmian)" :src="wupinchuzuRecommend[5].fengmian.split(',')[0]" alt="" />
				<img :style='{"width":"40%","objectFit":"cover","borderRadius":"15px","float":"left","height":"100%"}' v-else :src="baseUrl + (wupinchuzuRecommend[5].fengmian?wupinchuzuRecommend[5].fengmian.split(',')[0]:'')" alt="" />
				<div class="title line1" :style='{"overflow":"hidden","color":"#000","textAlign":"center","width":"55%","lineHeight":"180px","fontSize":"18px","height":"180px"}'>
                    <div>类别:{{wupinchuzuRecommend[5].leibie}}</div>
                    <div>联系人:{{wupinchuzuRecommend[5].lianxiren}}</div>
                </div>
			</div>
		</div>
	</div>
	
	
	
	<div @click="moreBtn('wupinchuzu')" :style='{"border":"0","boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","margin":"30px auto","borderRadius":"10px","textAlign":"center","display":"block","width":"150px","lineHeight":"32px"}'>
		<span :style='{"color":"#000","fontSize":"14px"}'>查看更多</span>
		<i v-if="true" :style='{"color":"#000","fontSize":"14px"}' class="el-icon-d-arrow-right"></i>
	</div>
	
</div>

	
<div class="news" :style='{"margin":"0 0 10px","backgroundSize":"100% 100%","backgroundImage":"url(http://codegen.caihongy.cn/20220914/0863c54d16aa49d0881cd858052fb45d.png)","height":"940px"}'>
	<div v-if="true" class="idea newsIdea" :style='{"padding":"20px","flexWrap":"wrap","backgroundImage":"url(http://codegen.caihongy.cn/20220914/3dcc11e098f843188d36a4ab92a938e7.png)","justifyContent":"space-between","display":"flex","height":"392px"}'>
		<div class="box1" :style='{"width":"0","background":"#fff","height":"0"}'></div>
		<div class="box2" :style='{"width":"0","background":"#fff","height":"0"}'></div>
		<div class="box3" :style='{"width":"0","background":"#fff","height":"0"}'></div>
		<div class="box4" :style='{"width":"0","background":"#fff","height":"0"}'></div>
		<div class="box5" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box6" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box7" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box8" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box9" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box10" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
	</div>
	
	<div class="title" :style='{"border":"5px solid rgba(67, 144, 165, 1)","width":"250px","margin":"30px auto 20px","lineHeight":"54px","textAlign":"center","background":"#fff"}'>
		<span :style='{"color":"#000","fontSize":"30px"}'>闲置资讯</span>
	</div>
	
	
	
	
	
	
	
	
	
	<!-- 样式八 -->
	<div v-if="newsList.length" class="list list8 index-pv1" :style='{"width":"100%","padding":"10px 420px","flexWrap":"wrap","display":"flex","height":"auto"}'>
	  <div @click="toDetail('newsDetail', newsList[0])" v-if="newsList.length>0" :style='{"padding":"10px","margin":"0 90px 10px 15px","borderRadius":"10px","background":"rgba(216, 216, 216, 1)","display":"flex","width":"465px","position":"relative","height":"168px"}' class="list-item animation-box">
	    <div :style='{"width":"400px","padding":"10px","height":"auto"}'>
	      <div :style='{"margin":"0 0 0 60px","fontSize":"16px","lineHeight":"28px","overflow":"hidden","color":"#000","height":"28px"}'>{{newsList[0].title}}</div>
	      <div :style='{"margin":"0 0 0 60px","fontSize":"14px","lineHeight":"24px","overflow":"hidden","color":"#666","height":"100px"}'>{{newsList[0].introduction}}</div>
	      <div :style='{"color":"#fff","alignItems":"center","textAlign":"center","display":"flex","top":"0","borderRadius":"15px","left":"0","flexWrap":"wrap","background":"rgba(67, 144, 165, 1)","width":"70px","fontSize":"12px","lineHeight":"24px","position":"absolute","height":"100%"}'>{{newsList[0].addtime.split(" ")[0]}}</div>
	    </div>
	    <img :style='{"objectFit":"cover","width":"190px","height":"100%"}' :src="baseUrl + newsList[0].picture" >
	  </div>
	  <div @click="toDetail('newsDetail', newsList[1])" v-if="newsList.length>1" :style='{"padding":"10px","margin":"0 10px 10px","borderRadius":"10px","background":"rgba(216, 216, 216, 1)","display":"flex","width":"465px","position":"relative","height":"168px"}' class="list-item animation-box">
	    <img :style='{"objectFit":"cover","width":"190px","height":"100%"}' :src="baseUrl + newsList[1].picture" >
	    <div :style='{"width":"400px","padding":"10px","height":"auto"}'>
	      <div :style='{"width":"80%","fontSize":"16px","lineHeight":"28px","overflow":"hidden","color":"#000","height":"30px"}'>{{newsList[1].title}}</div>
	      <div :style='{"width":"80%","fontSize":"14px","lineHeight":"24px","overflow":"hidden","color":"#666","height":"100px"}'>{{newsList[1].introduction}}</div>
	      <div :style='{"color":"#fff","alignItems":"center","textAlign":"center","display":"flex","right":"0","top":"0","borderRadius":"15px","flexWrap":"wrap","background":"#4390a5","width":"70px","fontSize":"12px","lineHeight":"24px","position":"absolute","height":"168px"}'>{{newsList[1].addtime.split(" ")[0]}}</div>
	    </div>
	  </div>
	  <div @click="toDetail('newsDetail', newsList[2])" v-if="newsList.length>2" :style='{"padding":"10px","margin":"0 90px 10px 15px","borderRadius":"10px","background":"rgba(216, 216, 216, 1)","display":"flex","width":"465px","position":"relative","height":"168px"}' class="list-item animation-box">
	    <div :style='{"width":"400px","padding":"10px","height":"auto"}'>
	      <div :style='{"margin":"0 0 0 60px","fontSize":"16px","lineHeight":"28px","overflow":"hidden","color":"#000","height":"30px"}'>{{newsList[2].title}}</div>
	      <div :style='{"margin":"0 0 0 60px","fontSize":"14px","lineHeight":"24px","overflow":"hidden","color":"#666","height":"100px"}'>{{newsList[2].introduction}}</div>
	      <div :style='{"color":"#fff","alignItems":"center","textAlign":"center","display":"flex","top":"0","borderRadius":"15px","left":"0","flexWrap":"wrap","background":"#4390a5","width":"70px","fontSize":"12px","lineHeight":"24px","position":"absolute","height":"168px"}'>{{newsList[2].addtime.split(" ")[0]}}</div>
	    </div>
	    <img :style='{"objectFit":"cover","width":"190px","height":"100%"}' :src="baseUrl + newsList[2].picture" >
	  </div>
	  <div @click="toDetail('newsDetail', newsList[3])" v-if="newsList.length>3" :style='{"padding":"10px","margin":"0 10px","borderRadius":"10px","background":"rgba(216, 216, 216, 1)","display":"flex","width":"465px","position":"relative","height":"168px"}' class="list-item animation-box">
	    <img :style='{"objectFit":"cover","width":"190px","height":"100%"}' :src="baseUrl + newsList[3].picture" >
	    <div :style='{"width":"400px","padding":"10px","height":"auto"}'>
	      <div :style='{"width":"80%","fontSize":"16px","lineHeight":"28px","overflow":"hidden","color":"#000","height":"30px"}'>{{newsList[3].title}}</div>
	      <div :style='{"width":"80%","fontSize":"14px","lineHeight":"24px","overflow":"hidden","color":"#666","height":"100px"}'>{{newsList[3].introduction}}</div>
	      <div :style='{"color":"#fff","alignItems":"center","textAlign":"center","display":"flex","right":"0","top":"0","borderRadius":"15px","flexWrap":"wrap","background":"#4390a5","width":"70px","fontSize":"12px","lineHeight":"24px","position":"absolute","height":"168px"}'>{{newsList[3].addtime.split(" ")[0]}}</div>
	    </div>
	  </div>
	</div>
	
	
	
	<div @click="moreBtn('news')" :style='{"border":"0","margin":"10px auto","borderRadius":"10px","textAlign":"center","background":"rgba(67, 144, 165, 1)","display":"block","width":"80px","lineHeight":"32px"}'>
		<span :style='{"color":"#f5f5f5","fontSize":"12px"}'>查看更多</span>
		<i v-if="true" :style='{"color":"#f5f5f5","fontSize":"12px"}' class="el-icon-d-arrow-right"></i>
	</div>
	
</div>


<div class="lists" :style='{"padding":"100px 0","margin":"0 0 10px","background":"#fff","height":"900px"}'>
	<div v-if="false" class="idea" :style='{"padding":"20px","flexWrap":"wrap","background":"#fff","justifyContent":"space-between","display":"flex","height":"100px"}'>
		<div class="box1" :style='{"width":"20%","background":"#fff","height":"80px"}'></div>
		<div class="box2" :style='{"width":"20%","background":"#fff","height":"80px"}'></div>
		<div class="box3" :style='{"width":"20%","background":"#fff","height":"80px"}'></div>
		<div class="box4" :style='{"width":"20%","background":"#fff","height":"80px"}'></div>
		<div class="box5" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box6" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box7" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box8" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box9" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
		<div class="box10" :style='{"width":"20%","background":"#fff","display":"none","height":"80px"}'></div>
	</div>
	
	<div class="title" :style='{"width":"200px","margin":"10px auto 40px","lineHeight":"54px","textAlign":"center"}'>
	  <span :style='{"color":"rgba(0, 0, 0, 1)","fontSize":"30px"}'>物品信息展示</span>
	</div>
	
	
	
	
	
	<!-- 样式四 -->
	<div class="list list4 index-pv1" :style='{"width":"100%","padding":"10px 460px","position":"relative","background":"#fff","height":"auto"}'>
		<div v-if="wupinxinxiList.length>0" class="list-4-item animation-box" @click="toDetail('wupinxinxiDetail', wupinxinxiList[0])" :style='{"boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","margin":"0 10px","overflow":"hidden","borderRadius":"15px","background":"#777","width":"320px","position":"relative","float":"left","height":"450px"}'>
			<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"80%"}' v-if="preHttp(wupinxinxiList[0].wupintupian)" :src="wupinxinxiList[0].wupintupian.split(',')[0]" alt="" />
			<img :style='{"width":"100%","objectFit":"cover","borderRadius":"15px","float":"left","height":"80%"}' v-else :src="baseUrl + (wupinxinxiList[0].wupintupian?wupinxinxiList[0].wupintupian.split(',')[0]:'')" alt="" />
			<div class="list-4-item-title line1" :style='{"overflow":"hidden","color":"#fff","textAlign":"center","background":"#777","width":"100%","lineHeight":"90px","fontSize":"18px","height":"90px"}'>
                <div>物品名称:{{wupinxinxiList[0].wupinmingcheng}}</div>
                <div>品牌:{{wupinxinxiList[0].pinpai}}</div>
                <div>物品类别:{{wupinxinxiList[0].wupinleibie}}</div>
            </div>
		</div>
		<div :style='{"width":"140px","margin":"0 0 10px","display":"inline-block","height":"275px"}' class="list-4-body">
			<div v-if="wupinxinxiList.length>1" @click="toDetail('wupinxinxiDetail', wupinxinxiList[1])" class="list-4-item animation-box item-2" :style='{"boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","margin":"0 10px 10px","overflow":"hidden","borderRadius":"15px","display":"inline-block","width":"660px","position":"relative","height":"206px"}'>
				<img :style='{"width":"80%","objectFit":"cover","float":"left","height":"100%"}' v-if="preHttp(wupinxinxiList[1].wupintupian)" :src="wupinxinxiList[1].wupintupian.split(',')[0]" alt="" />
				<img :style='{"width":"80%","objectFit":"cover","float":"left","height":"100%"}' v-else :src="baseUrl + (wupinxinxiList[1].wupintupian?wupinxinxiList[1].wupintupian.split(',')[0]:'')" alt="" />
				<div class="list-4-item-title line1" :style='{"padding":"20px 10px","overflow":"hidden","color":"#fff","top":"0","textAlign":"center","background":"rgba(0, 0, 0, 0.5000)","width":"132px","lineHeight":"170px","fontSize":"18px","right":"0","height":"100%"}'>
                    <div>物品名称:{{wupinxinxiList[1].wupinmingcheng}}</div>
                    <div>品牌:{{wupinxinxiList[1].pinpai}}</div>
                    <div>物品类别:{{wupinxinxiList[1].wupinleibie}}</div>
                </div>
			</div>
			<div v-if="wupinxinxiList.length>2" @click="toDetail('wupinxinxiDetail', wupinxinxiList[2])" class="list-4-item animation-box item-3" :style='{"boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","margin":"0 10px","overflow":"hidden","borderRadius":"15px","background":"#777","display":"inline-block","width":"210px","position":"relative","height":"230px"}'>
				<img :style='{"width":"100%","objectFit":"cover","float":"left","height":"80%"}' v-if="preHttp(wupinxinxiList[2].wupintupian)" :src="wupinxinxiList[2].wupintupian.split(',')[0]" alt="" />
				<img :style='{"width":"100%","objectFit":"cover","float":"left","height":"80%"}' v-else :src="baseUrl + (wupinxinxiList[2].wupintupian?wupinxinxiList[2].wupintupian.split(',')[0]:'')" alt="" />
				<div class="list-4-item-title line1" :style='{"overflow":"hidden","color":"#fff","textAlign":"center","background":"#777","width":"100%","lineHeight":"46px","fontSize":"18px","height":"46px"}'>
                    <div>物品名称:{{wupinxinxiList[2].wupinmingcheng}}</div>
                    <div>品牌:{{wupinxinxiList[2].pinpai}}</div>
                    <div>物品类别:{{wupinxinxiList[2].wupinleibie}}</div>
                </div>
			</div>
		</div>
		<div :style='{"margin":"10px 0 0","bottom":"12px","display":"inline-block","width":"550px","position":"absolute","right":"330px","height":"230px"}' class="list-4-body">
			<div v-if="wupinxinxiList.length>3" @click="toDetail('wupinxinxiDetail', wupinxinxiList[3])" class="list-4-item animation-box item-4" :style='{"boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","margin":"0 10px","overflow":"hidden","borderRadius":"15px","background":"#777","display":"inline-block","width":"210px","position":"relative","height":"230px"}'>
				<img :style='{"width":"100%","objectFit":"cover","float":"left","height":"80%"}' v-if="preHttp(wupinxinxiList[3].wupintupian)" :src="wupinxinxiList[3].wupintupian.split(',')[0]" alt="" />
				<img :style='{"width":"100%","objectFit":"cover","float":"left","height":"80%"}' v-else :src="baseUrl + (wupinxinxiList[3].wupintupian?wupinxinxiList[3].wupintupian.split(',')[0]:'')" alt="" />
				<div class="list-4-item-title line1" :style='{"overflow":"hidden","color":"#fff","textAlign":"center","background":"#777","width":"100%","lineHeight":"46px","fontSize":"18px","height":"46px"}'>
                    <div>物品名称:{{wupinxinxiList[3].wupinmingcheng}}</div>
                    <div>品牌:{{wupinxinxiList[3].pinpai}}</div>
                    <div>物品类别:{{wupinxinxiList[3].wupinleibie}}</div>
                </div>
			</div>
			<div v-if="wupinxinxiList.length>4" @click="toDetail('wupinxinxiDetail', wupinxinxiList[4])" class="list-4-item animation-box item-5" :style='{"boxShadow":"0px 4px 10px 0px rgba(0,0,0,0.302)","margin":"0 10px","overflow":"hidden","borderRadius":"15px","background":"#777777","display":"inline-block","width":"210px","position":"relative","height":"230px"}'>
				<img :style='{"width":"100%","objectFit":"cover","float":"left","height":"80%"}' v-if="preHttp(wupinxinxiList[4].wupintupian)" :src="wupinxinxiList[4].wupintupian.split(',')[0]" alt="" />
				<img :style='{"width":"100%","objectFit":"cover","float":"left","height":"80%"}' v-else :src="baseUrl + (wupinxinxiList[4].wupintupian?wupinxinxiList[4].wupintupian.split(',')[0]:'')" alt="" />
				<div class="list-4-item-title line1" :style='{"overflow":"hidden","color":"#fff","textAlign":"center","background":"#777777","width":"100%","lineHeight":"46px","fontSize":"18px","height":"46px"}'>
                    <div>物品名称:{{wupinxinxiList[4].wupinmingcheng}}</div>
                    <div>品牌:{{wupinxinxiList[4].pinpai}}</div>
                    <div>物品类别:{{wupinxinxiList[4].wupinleibie}}</div>
                </div>
			</div>
		</div>
	</div>
	
	
	
	
	
	
	<div @click="moreBtn('wupinxinxi')" :style='{"border":"0","margin":"10px auto","borderRadius":"10px","textAlign":"center","background":"rgba(0, 0, 0, 0.5000)","display":"block","width":"150px","lineHeight":"32px"}'>
		<span :style='{"color":"#f5f5f5","fontSize":"12px"}'>查看更多</span>
		<i v-if="true" :style='{"color":"#f5f5f5","fontSize":"12px"}' class="el-icon-d-arrow-right"></i>
	</div>
	

</div>


</div>
</template>

<script>
  export default {
    //数据集合
    data() {
      return {
        baseUrl: '',
        aboutUsDetail: {},
        systemIntroductionDetail: {},
        newsList: [],
        wupinxinxiRecommend: [],
        wupinchuzuRecommend: [],

        wupinxinxiList: [],
      }
    },
    created() {
      this.baseUrl = this.$config.baseUrl;
      this.getNewsList();
      this.getAboutUs();
      this.getSystemIntroduction();
      this.getList();
    },
    //方法集合
    methods: {
      preHttp(str) {
          return str && str.substr(0,4)=='http';
      },
      getAboutUs() {
          this.$http.get('aboutus/detail/1', {}).then(res => {
            if(res.data.code == 0) {
              this.aboutUsDetail = res.data.data;
            }
          })
      },
      getSystemIntroduction() {
          this.$http.get('systemintro/detail/1', {}).then(res => {
            if(res.data.code == 0) {
              this.systemIntroductionDetail = res.data.data;
            }
          })
      },
		getNewsList() {
			this.$http.get('news/list', {params: {
				page: 1,
				limit: 6,
                sort: 'addtime',
			order: 'desc'}}).then(res => {
				if (res.data.code == 0) {
					this.newsList = res.data.data.list;
					
					
				}
			});
		},
		getList() {
          let autoSortUrl = "";
          autoSortUrl = "wupinxinxi/autoSort";
          if(localStorage.getItem('Token')) {
              autoSortUrl = "wupinxinxi/autoSort2";
          }
			this.$http.get(autoSortUrl, {params: {
				page: 1,
				limit: 6,
			}}).then(res => {
				if (res.data.code == 0) {
					this.wupinxinxiRecommend = res.data.data.list;
					
					
					// 商品列表样式五
					
				}
			});
          autoSortUrl = "wupinchuzu/autoSort";
			this.$http.get(autoSortUrl, {params: {
				page: 1,
				limit: 6,
			}}).then(res => {
				if (res.data.code == 0) {
					this.wupinchuzuRecommend = res.data.data.list;
					
					
					// 商品列表样式五
					
				}
			});
			
			this.$http.get('wupinxinxi/list', {params: {
				page: 1,
				limit: 6,
			}}).then(res => {
				if (res.data.code == 0) {
					this.wupinxinxiList = res.data.data.list;
					
					// 商品列表样式五
					
				}
			});
		},
		toDetail(path, item) {
			this.$router.push({path: '/index/' + path, query: {detailObj: JSON.stringify(item)}});
		},
		moreBtn(path) {
			this.$router.push({path: '/index/' + path});
		}
    }
  }
</script>

<style rel="stylesheet/scss" lang="scss" scoped>
	.home-preview {
	
		.recommend {
			.list3 .swiper-button-prev {
				left: 10px;
				right: auto;
			}
			
			.list3 .swiper-button-prev::after {
				color: rgb(64, 158, 255);
			}
			
			.list3 .swiper-button-next {
				left: auto;
				right: 10px;
			}
			
			.list3 .swiper-button-next::after {
				color: rgb(64, 158, 255);
			}
			
			.list5 .swiper-button-prev {
				left: 10px;
				right: auto;
			}
			
			.list5 .swiper-button-prev::after {
				color: rgb(64, 158, 255);
        }
        
        .list5 .swiper-button-next {
				left: auto;
				right: 10px;
			}
			
			.list5 .swiper-button-next::after {
				color: rgb(64, 158, 255);
			}
			
			.list5 {
				.swiper-slide-prev {
					position: relative;
					z-index: 3;
				}
		
				.swiper-slide-next {
					position: relative;
					z-index: 3;
				}
		
				.swiper-slide-active {
					position: relative;
					z-index: 5;
				}
			}
			
			.index-pv1 .animation-box {
				transform: rotate(0deg) scale(1) skew(0deg, 0deg) translate3d(0px, 0px, 0px);
				z-index: initial;
			}
			
			.index-pv1 .animation-box:hover {
				transform: scale(1.05) skew(0deg, 0deg) translate3d(0px, 0px, 0px);
				-webkit-perspective: 1000px;
				perspective: 1000px;
				transition: 0.3s;
				z-index: 1;
			}
			
			.index-pv1 .animation-box img {
				transform: rotate(0deg) scale(1) skew(0deg, 0deg) translate3d(0px, 0px, 0px);
			}
			
			.index-pv1 .animation-box img:hover {
				-webkit-perspective: 1000px;
				perspective: 1000px;
				transition: 0.3s;
			}
		}
		
		.news {
			.list3 .swiper-button-prev {
				left: 10px;
				right: auto;
			}
			
			.list3 .swiper-button-prev::after {
				color: rgb(64, 158, 255);
			}
			
			.list3 .swiper-button-next {
				left: auto;
				right: 10px;
			}
			
			.list3 .swiper-button-next::after {
				color: rgb(64, 158, 255);
			}
			
			.list6 .swiper-button-prev {
				left: 10px;
				right: auto;
			}
			
			.list6 .swiper-button-prev::after {
				color: rgb(64, 158, 255);
			}
			
			.list6 .swiper-button-next {
				left: auto;
				right: 10px;
			}
			
			.list6 .swiper-button-next::after {
				color: rgb(64, 158, 255);
			}
			
			.index-pv1 .animation-box {
				transform: rotate(0deg) scale(1) skew(0deg, 0deg) translate3d(0px, 0px, 0px);
				z-index: initial;
			}
			
			.index-pv1 .animation-box:hover {
				transform: scale(1.05) skew(0deg, 0deg) translate3d(0px, 0px, 0px);
				-webkit-perspective: 1000px;
				perspective: 1000px;
				transition: 0.3s;
				z-index: 1;
			}
			
			.index-pv1 .animation-box img {
				transform: rotate(0deg) scale(1) skew(0deg, 0deg) translate3d(0px, 0px, 0px);
			}
			
			.index-pv1 .animation-box img:hover {
				-webkit-perspective: 1000px;
				perspective: 1000px;
				transition: 0.3s;
			}
		}
	
		.lists {
			.list3 .swiper-button-prev {
				left: 10px;
				right: auto;
			}
			
			.list3 .swiper-button-prev::after {
				color: rgb(64, 158, 255);
			}
			
			.list3 .swiper-button-next {
				left: auto;
				right: 10px;
        }
        
        .list3 .swiper-button-next::after {
				color: rgb(64, 158, 255);
			}
			
			.list5 .swiper-button-prev {
				left: 10px;
				right: auto;
			}
			
			.list5 .swiper-button-prev::after {
				color: rgb(64, 158, 255);
			}
			
			.list5 .swiper-button-next {
            left: auto;
            right: 10px;
			}
			
			.list5 .swiper-button-next::after {
				color: rgb(64, 158, 255);
			}
			
			.list5 {
				.swiper-slide-prev {
					position: relative;
					z-index: 3;
				}
		
				.swiper-slide-next {
					position: relative;
					z-index: 3;
				}
		
				.swiper-slide-active {
					position: relative;
					z-index: 5;
				}
			}
			
			.index-pv1 .animation-box {
				transform: rotate(0deg) scale(1) skew(0deg, 0deg) translate3d(0px, 0px, 0px);
				z-index: initial;
			}
			
			.index-pv1 .animation-box:hover {
				transform: scale(1.03) skew(0deg, 0deg) translate3d(0px, 0px, 0px);
				-webkit-perspective: 1000px;
				perspective: 1000px;
				transition: 0.3s;
				z-index: 1;
			}
			
			.index-pv1 .animation-box img {
				transform: rotate(0deg) scale(1) skew(0deg, 0deg) translate3d(0px, 0px, 0px);
			}
			
			.index-pv1 .animation-box img:hover {
				-webkit-perspective: 1000px;
				perspective: 1000px;
				transition: 0.3s;
			}
		}
	}
</style>
