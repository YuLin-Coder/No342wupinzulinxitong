<template>
	<div class="addEdit-block" :style='{"padding":"30px 0 0 0"}' style="width: 100%;">
		<el-form
			:style='{"padding":"20px 0px 120px"}'
			class="add-update-preview"
			ref="ruleForm"
			:model="ruleForm"
			:rules="rules"
			label-width="140px"
		>
			<template >
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="input" v-if="type!='info'"  label="物品编号" prop="wupinbianhao">
					<el-input v-model="ruleForm.wupinbianhao" placeholder="物品编号" clearable  :readonly="ro.wupinbianhao"></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' v-else class="input" label="物品编号" prop="wupinbianhao">
					<el-input v-model="ruleForm.wupinbianhao" placeholder="物品编号" readonly></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="input" v-if="type!='info'"  label="物品名称" prop="wupinmingcheng">
					<el-input v-model="ruleForm.wupinmingcheng" placeholder="物品名称" clearable  :readonly="ro.wupinmingcheng"></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' v-else class="input" label="物品名称" prop="wupinmingcheng">
					<el-input v-model="ruleForm.wupinmingcheng" placeholder="物品名称" readonly></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="input" v-if="type!='info'"  label="品牌" prop="pinpai">
					<el-input v-model="ruleForm.pinpai" placeholder="品牌" clearable  :readonly="ro.pinpai"></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' v-else class="input" label="品牌" prop="pinpai">
					<el-input v-model="ruleForm.pinpai" placeholder="品牌" readonly></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="input" v-if="type!='info'"  label="物品类别" prop="wupinleibie">
					<el-input v-model="ruleForm.wupinleibie" placeholder="物品类别" clearable  :readonly="ro.wupinleibie"></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' v-else class="input" label="物品类别" prop="wupinleibie">
					<el-input v-model="ruleForm.wupinleibie" placeholder="物品类别" readonly></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="upload" v-if="type!='info' && !ro.wupintupian" label="物品图片" prop="wupintupian">
					<file-upload
						tip="点击上传物品图片"
						action="file/upload"
						:limit="3"
						:multiple="true"
						:fileUrls="ruleForm.wupintupian?ruleForm.wupintupian:''"
						@change="wupintupianUploadChange"
					></file-upload>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="upload" v-else-if="ruleForm.wupintupian" label="物品图片" prop="wupintupian">
					<img v-if="ruleForm.wupintupian.substring(0,4)=='http'" class="upload-img" style="margin-right:20px;" v-bind:key="index" :src="ruleForm.wupintupian.split(',')[0]" width="100" height="100">
					<img v-else class="upload-img" style="margin-right:20px;" v-bind:key="index" v-for="(item,index) in ruleForm.wupintupian.split(',')" :src="$base.url+item" width="100" height="100">
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="input" v-if="type!='info'"  label="归还数量" prop="wupinshuliang">
					<el-input v-model="ruleForm.wupinshuliang" placeholder="归还数量" clearable  :readonly="ro.wupinshuliang"></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' v-else class="input" label="归还数量" prop="wupinshuliang">
					<el-input v-model="ruleForm.wupinshuliang" placeholder="归还数量" readonly></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="input" v-if="type!='info'"  label="租金" prop="zujin">
					<el-input v-model="ruleForm.zujin" placeholder="租金" clearable  :readonly="ro.zujin"></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' v-else class="input" label="租金" prop="zujin">
					<el-input v-model="ruleForm.zujin" placeholder="租金" readonly></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="input" v-if="type!='info'"  label="租赁时间" prop="zulinshijian">
					<el-input v-model="ruleForm.zulinshijian" placeholder="租赁时间" clearable  :readonly="ro.zulinshijian"></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' v-else class="input" label="租赁时间" prop="zulinshijian">
					<el-input v-model="ruleForm.zulinshijian" placeholder="租赁时间" readonly></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="input" v-if="type!='info'"  label="押金" prop="yajin">
					<el-input v-model="ruleForm.yajin" placeholder="押金" clearable  :readonly="ro.yajin"></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' v-else class="input" label="押金" prop="yajin">
					<el-input v-model="ruleForm.yajin" placeholder="押金" readonly></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="input" v-if="type!='info'"  label="总价格" prop="zongjiage">
					<el-input v-model="zongjiage" placeholder="总价格" readonly></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="input" v-else-if="ruleForm.zongjiage" label="总价格" prop="zongjiage">
					<el-input v-model="ruleForm.zongjiage" placeholder="总价格" readonly></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="date" v-if="type!='info'" label="归还时间" prop="guihaishijian">
					<el-date-picker
						format="yyyy 年 MM 月 dd 日"
						value-format="yyyy-MM-dd"
						v-model="ruleForm.guihaishijian" 
						type="date"
						:readonly="ro.guihaishijian"
						placeholder="归还时间"
					></el-date-picker> 
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="input" v-else-if="ruleForm.guihaishijian" label="归还时间" prop="guihaishijian">
					<el-input v-model="ruleForm.guihaishijian" placeholder="归还时间" readonly></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="input" v-if="type!='info'"  label="我的账号" prop="wodezhanghao">
					<el-input v-model="ruleForm.wodezhanghao" placeholder="我的账号" clearable  :readonly="ro.wodezhanghao"></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' v-else class="input" label="我的账号" prop="wodezhanghao">
					<el-input v-model="ruleForm.wodezhanghao" placeholder="我的账号" readonly></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="input" v-if="type!='info'"  label="我的姓名" prop="wodexingming">
					<el-input v-model="ruleForm.wodexingming" placeholder="我的姓名" clearable  :readonly="ro.wodexingming"></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' v-else class="input" label="我的姓名" prop="wodexingming">
					<el-input v-model="ruleForm.wodexingming" placeholder="我的姓名" readonly></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="input" v-if="type!='info'"  label="我的电话" prop="wodedianhua">
					<el-input v-model="ruleForm.wodedianhua" placeholder="我的电话" clearable  :readonly="ro.wodedianhua"></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' v-else class="input" label="我的电话" prop="wodedianhua">
					<el-input v-model="ruleForm.wodedianhua" placeholder="我的电话" readonly></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="select" v-if="type!='info'" label="出租账号" prop="yonghuzhanghao">
					<el-select :disabled="ro.yonghuzhanghao" @change="yonghuzhanghaoChange" v-model="ruleForm.yonghuzhanghao" placeholder="请选择出租账号">
						<el-option
							v-for="(item,index) in yonghuzhanghaoOptions"
							v-bind:key="index"
							:label="item"
							:value="item">
						</el-option>
					</el-select>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="input" v-else-if="ruleForm.yonghuzhanghao" label="出租账号" prop="yonghuzhanghao">
					<el-input v-model="ruleForm.yonghuzhanghao" placeholder="出租账号" readonly></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="input" v-if="type!='info'"  label="出租姓名" prop="yonghuxingming">
					<el-input v-model="ruleForm.yonghuxingming" placeholder="出租姓名" clearable  :readonly="ro.yonghuxingming"></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' v-else class="input" label="出租姓名" prop="yonghuxingming">
					<el-input v-model="ruleForm.yonghuxingming" placeholder="出租姓名" readonly></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="input" v-if="type!='info'"  label="出租电话" prop="yonghudianhua">
					<el-input v-model="ruleForm.yonghudianhua" placeholder="出租电话" clearable  :readonly="ro.yonghudianhua"></el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' v-else class="input" label="出租电话" prop="yonghudianhua">
					<el-input v-model="ruleForm.yonghudianhua" placeholder="出租电话" readonly></el-input>
				</el-form-item>
			</template>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' class="textarea" v-if="type!='info'" label="物品状态" prop="wupinzhuangtai">
					<el-input
					  style="min-width: 200px; max-width: 600px;"
					  type="textarea"
					  :rows="8"
					  placeholder="物品状态"
					  v-model="ruleForm.wupinzhuangtai" >
					</el-input>
				</el-form-item>
				<el-form-item :style='{"width":"100%","margin":"0 0 20px 0","display":"inline-block"}' v-else-if="ruleForm.wupinzhuangtai" label="物品状态" prop="wupinzhuangtai">
					<span :style='{"fontSize":"14px","lineHeight":"40px","color":"#333","fontWeight":"500","display":"inline-block"}'>{{ruleForm.wupinzhuangtai}}</span>
				</el-form-item>
			<el-form-item :style='{"padding":"0","margin":"0"}' class="btn">
				<el-button :style='{"border":"0","cursor":"pointer","padding":"0","margin":"0 20px 0 0","outline":"none","color":"rgba(255, 255, 255, 1)","borderRadius":"40px","background":"rgba(27, 167, 166, 1)","width":"128px","lineHeight":"40px","fontSize":"14px","height":"40px"}'  v-if="type!='info'" type="primary" class="btn-success" @click="onSubmit">提交</el-button>
				<el-button :style='{"border":"1px solid rgba(27, 167, 166, 1)","cursor":"pointer","padding":"0","margin":"0","outline":"none","color":"rgba(27, 167, 166, 1)","borderRadius":"40px","background":"rgba(255, 255, 255, 1)","width":"128px","lineHeight":"40px","fontSize":"14px","height":"40px"}' v-if="type!='info'" class="btn-close" @click="back()">取消</el-button>
				<el-button :style='{"border":"1px solid rgba(27, 167, 166, 1)","cursor":"pointer","padding":"0","margin":"0","outline":"none","color":"rgba(27, 167, 166, 1)","borderRadius":"40px","background":"rgba(255, 255, 255, 1)","width":"128px","lineHeight":"40px","fontSize":"14px","height":"40px"}' v-if="type=='info'" class="btn-close" @click="back()">返回</el-button>
			</el-form-item>
		</el-form>
    

  </div>
</template>
<script>
// 数字，邮件，手机，url，身份证校验
import { isNumber,isIntNumer,isEmail,isPhone, isMobile,isURL,checkIdCard } from "@/utils/validate";
export default {
	data() {
		let self = this
		var validateIdCard = (rule, value, callback) => {
			if(!value){
				callback();
			} else if (!checkIdCard(value)) {
				callback(new Error("请输入正确的身份证号码"));
			} else {
				callback();
			}
		};
		var validateUrl = (rule, value, callback) => {
			if(!value){
				callback();
			} else if (!isURL(value)) {
				callback(new Error("请输入正确的URL地址"));
			} else {
				callback();
			}
		};
		var validateMobile = (rule, value, callback) => {
			if(!value){
				callback();
			} else if (!isMobile(value)) {
				callback(new Error("请输入正确的手机号码"));
			} else {
				callback();
			}
		};
		var validatePhone = (rule, value, callback) => {
			if(!value){
				callback();
			} else if (!isPhone(value)) {
				callback(new Error("请输入正确的电话号码"));
			} else {
				callback();
			}
		};
		var validateEmail = (rule, value, callback) => {
			if(!value){
				callback();
			} else if (!isEmail(value)) {
				callback(new Error("请输入正确的邮箱地址"));
			} else {
				callback();
			}
		};
		var validateNumber = (rule, value, callback) => {
			if(!value){
				callback();
			} else if (!isNumber(value)) {
				callback(new Error("请输入数字"));
			} else {
				callback();
			}
		};
		var validateIntNumber = (rule, value, callback) => {
			if(!value){
				callback();
			} else if (!isIntNumer(value)) {
				callback(new Error("请输入整数"));
			} else {
				callback();
			}
		};
		return {
			id: '',
			type: '',
			
			
			ro:{
				wupinbianhao : false,
				wupinmingcheng : false,
				pinpai : false,
				wupinleibie : false,
				wupintupian : false,
				wupinzhuangtai : false,
				wupinshuliang : false,
				zujin : false,
				zulinshijian : false,
				yajin : false,
				zongjiage : false,
				guihaishijian : false,
				wodezhanghao : false,
				wodexingming : false,
				wodedianhua : false,
				yonghuzhanghao : false,
				yonghuxingming : false,
				yonghudianhua : false,
				crossuserid : false,
				crossrefid : false,
				ispay : false,
			},
			
			
			ruleForm: {
				wupinbianhao: '',
				wupinmingcheng: '',
				pinpai: '',
				wupinleibie: '',
				wupintupian: '',
				wupinzhuangtai: '',
				wupinshuliang: '',
				zujin: '',
				zulinshijian: '',
				yajin: '',
				zongjiage: '',
				guihaishijian: '',
				wodezhanghao: '',
				wodexingming: '',
				wodedianhua: '',
				yonghuzhanghao: '',
				yonghuxingming: '',
				yonghudianhua: '',
				crossuserid: '',
				crossrefid: '',
			},
		
			yonghuzhanghaoOptions: [],
			
			rules: {
				wupinbianhao: [
				],
				wupinmingcheng: [
				],
				pinpai: [
				],
				wupinleibie: [
				],
				wupintupian: [
				],
				wupinzhuangtai: [
					{ required: true, message: '物品状态不能为空', trigger: 'blur' },
				],
				wupinshuliang: [
					{ required: true, message: '归还数量不能为空', trigger: 'blur' },
					{ validator: validateIntNumber, trigger: 'blur' },
				],
				zujin: [
					{ validator: validateIntNumber, trigger: 'blur' },
				],
				zulinshijian: [
				],
				yajin: [
					{ required: true, message: '押金不能为空', trigger: 'blur' },
					{ validator: validateIntNumber, trigger: 'blur' },
				],
				zongjiage: [
					{ validator: validateIntNumber, trigger: 'blur' },
				],
				guihaishijian: [
				],
				wodezhanghao: [
				],
				wodexingming: [
				],
				wodedianhua: [
				],
				yonghuzhanghao: [
				],
				yonghuxingming: [
				],
				yonghudianhua: [
				],
				crossuserid: [
				],
				crossrefid: [
				],
				ispay: [
				],
			}
		};
	},
	props: ["parent"],
	computed: {



		zongjiage:{
			
			get: function () {
				return 1*this.ruleForm.wupinshuliang*this.ruleForm.zujin-parseFloat(this.ruleForm.yajin==""?0:this.ruleForm.yajin)
			}
		},
	},
    components: {
    },
	created() {
		this.ruleForm.guihaishijian = this.getCurDate()
	},
	methods: {
		
		// 下载
		download(file){
			window.open(`${file}`)
		},
		// 初始化
		init(id,type) {
			if (id) {
				this.id = id;
				this.type = type;
			}
			if(this.type=='info'||this.type=='else'){
				this.info(id);
			}else if(this.type=='logistics'){
				this.logistics=false;
				this.info(id);
			}else if(this.type=='cross'){
				var obj = this.$storage.getObj('crossObj');
				for (var o in obj){
						if(o=='wupinbianhao'){
							this.ruleForm.wupinbianhao = obj[o];
							this.ro.wupinbianhao = true;
							continue;
						}
						if(o=='wupinmingcheng'){
							this.ruleForm.wupinmingcheng = obj[o];
							this.ro.wupinmingcheng = true;
							continue;
						}
						if(o=='pinpai'){
							this.ruleForm.pinpai = obj[o];
							this.ro.pinpai = true;
							continue;
						}
						if(o=='wupinleibie'){
							this.ruleForm.wupinleibie = obj[o];
							this.ro.wupinleibie = true;
							continue;
						}
						if(o=='wupintupian'){
							this.ruleForm.wupintupian = obj[o];
							this.ro.wupintupian = true;
							continue;
						}
						if(o=='wupinzhuangtai'){
							this.ruleForm.wupinzhuangtai = obj[o];
							this.ro.wupinzhuangtai = true;
							continue;
						}
						if(o=='wupinshuliang'){
							this.ruleForm.wupinshuliang = obj[o];
							this.ro.wupinshuliang = true;
							continue;
						}
						if(o=='zujin'){
							this.ruleForm.zujin = obj[o];
							this.ro.zujin = true;
							continue;
						}
						if(o=='zulinshijian'){
							this.ruleForm.zulinshijian = obj[o];
							this.ro.zulinshijian = true;
							continue;
						}
						if(o=='yajin'){
							this.ruleForm.yajin = obj[o];
							this.ro.yajin = true;
							continue;
						}
						if(o=='zongjiage'){
							this.ruleForm.zongjiage = obj[o];
							this.ro.zongjiage = true;
							continue;
						}
						if(o=='guihaishijian'){
							this.ruleForm.guihaishijian = obj[o];
							this.ro.guihaishijian = true;
							continue;
						}
						if(o=='wodezhanghao'){
							this.ruleForm.wodezhanghao = obj[o];
							this.ro.wodezhanghao = true;
							continue;
						}
						if(o=='wodexingming'){
							this.ruleForm.wodexingming = obj[o];
							this.ro.wodexingming = true;
							continue;
						}
						if(o=='wodedianhua'){
							this.ruleForm.wodedianhua = obj[o];
							this.ro.wodedianhua = true;
							continue;
						}
						if(o=='yonghuzhanghao'){
							this.ruleForm.yonghuzhanghao = obj[o];
							this.ro.yonghuzhanghao = true;
							continue;
						}
						if(o=='yonghuxingming'){
							this.ruleForm.yonghuxingming = obj[o];
							this.ro.yonghuxingming = true;
							continue;
						}
						if(o=='yonghudianhua'){
							this.ruleForm.yonghudianhua = obj[o];
							this.ro.yonghudianhua = true;
							continue;
						}
						if(o=='crossuserid'){
							this.ruleForm.crossuserid = obj[o];
							this.ro.crossuserid = true;
							continue;
						}
						if(o=='crossrefid'){
							this.ruleForm.crossrefid = obj[o];
							this.ro.crossrefid = true;
							continue;
						}
				}
				





















			}
			
			
			// 获取用户信息
			this.$http({
				url: `${this.$storage.get('sessionTable')}/session`,
				method: "get"
			}).then(({ data }) => {
				if (data && data.code === 0) {
					
					var json = data.data;
					if(((json.yonghuzhanghao!=''&&json.yonghuzhanghao) || json.yonghuzhanghao==0) && this.$storage.get("role")!="管理员"){
						this.ruleForm.wodezhanghao = json.yonghuzhanghao
						this.ro.wodezhanghao = true;
					}
					if(((json.yonghuxingming!=''&&json.yonghuxingming) || json.yonghuxingming==0) && this.$storage.get("role")!="管理员"){
						this.ruleForm.wodexingming = json.yonghuxingming
						this.ro.wodexingming = true;
					}
					if(((json.yonghudianhua!=''&&json.yonghudianhua) || json.yonghudianhua==0) && this.$storage.get("role")!="管理员"){
						this.ruleForm.wodedianhua = json.yonghudianhua
						this.ro.wodedianhua = true;
					}
				} else {
					this.$message.error(data.msg);
				}
			});
			
            this.$http({
				url: `option/yonghu/yonghuzhanghao`,
				method: "get"
            }).then(({ data }) => {
				if (data && data.code === 0) {
					this.yonghuzhanghaoOptions = data.data;
				} else {
					this.$message.error(data.msg);
				}
            });
			
		},
			// 下二随
			yonghuzhanghaoChange () {
				this.$http({
					url: `follow/yonghu/yonghuzhanghao?columnValue=`+ this.ruleForm.yonghuzhanghao,
					method: "get"
				}).then(({ data }) => {
					if (data && data.code === 0) {
						if(data.data.yonghuxingming){
							this.ruleForm.yonghuxingming = data.data.yonghuxingming
						}
						if(data.data.yonghudianhua){
							this.ruleForm.yonghudianhua = data.data.yonghudianhua
						}
					} else {
						this.$message.error(data.msg);
					}
				});
			},
    // 多级联动参数

    info(id) {
      this.$http({
        url: `wupinguihai/info/${id}`,
        method: "get"
      }).then(({ data }) => {
        if (data && data.code === 0) {
        this.ruleForm = data.data;
        //解决前台上传图片后台不显示的问题
        let reg=new RegExp('../../../upload','g')//g代表全部
        } else {
          this.$message.error(data.msg);
        }
      });
    },


    // 提交
    onSubmit() {
        this.ruleForm.zongjiage = this.zongjiage










	if(this.ruleForm.wupintupian!=null) {
		this.ruleForm.wupintupian = this.ruleForm.wupintupian.replace(new RegExp(this.$base.url,"g"),"");
	}

































var objcross = this.$storage.getObj('crossObj');

      //更新跨表属性
       var crossuserid;
       var crossrefid;
       var crossoptnum;
       if(this.type=='cross'){
                var statusColumnName = this.$storage.get('statusColumnName');
                var statusColumnValue = this.$storage.get('statusColumnValue');
                if(statusColumnName!='') {
                        var obj = this.$storage.getObj('crossObj');
                       if(statusColumnName && !statusColumnName.startsWith("[")) {
                               for (var o in obj){
                                 if(o==statusColumnName){
                                   obj[o] = statusColumnValue;
                                 }
                               }
                               var table = this.$storage.get('crossTable');
                             this.$http({
                                 url: `${table}/update`,
                                 method: "post",
                                 data: obj
                               }).then(({ data }) => {});
                       } else {
                               crossuserid=this.$storage.get('userid');
                               crossrefid=obj['id'];
                               crossoptnum=this.$storage.get('statusColumnName');
                               crossoptnum=crossoptnum.replace(/\[/,"").replace(/\]/,"");
                        }
                }
        }
       this.$refs["ruleForm"].validate(valid => {
         if (valid) {
		 if(crossrefid && crossuserid) {
			 this.ruleForm.crossuserid = crossuserid;
			 this.ruleForm.crossrefid = crossrefid;
			let params = { 
				page: 1, 
				limit: 10, 
				crossuserid:this.ruleForm.crossuserid,
				crossrefid:this.ruleForm.crossrefid,
			} 
			this.$http({ 
				url: "wupinguihai/page", 
				method: "get", 
				params: params 
			}).then(({ 
				data 
			}) => { 
				if (data && data.code === 0) { 
				       if(data.data.total>=crossoptnum) {
					     this.$message.error(this.$storage.get('tips'));
					       return false;
				       } else {
					 this.$http({
					   url: `wupinguihai/${!this.ruleForm.id ? "save" : "update"}`,
					   method: "post",
					   data: this.ruleForm
					 }).then(({ data }) => {
					   if (data && data.code === 0) {
					     this.$message({
					       message: "操作成功",
					       type: "success",
					       duration: 1500,
					       onClose: () => {
						 this.parent.showFlag = true;
						 this.parent.addOrUpdateFlag = false;
						 this.parent.wupinguihaiCrossAddOrUpdateFlag = false;
						 this.parent.search();
						 this.parent.contentStyleChange();
					       }
					     });
					   } else {
					     this.$message.error(data.msg);
					   }
					 });

				       }
				} else { 
				} 
			});
		 } else {
			 this.$http({
			   url: `wupinguihai/${!this.ruleForm.id ? "save" : "update"}`,
			   method: "post",
			   data: this.ruleForm
			 }).then(({ data }) => {
			   if (data && data.code === 0) {
			     this.$message({
			       message: "操作成功",
			       type: "success",
			       duration: 1500,
			       onClose: () => {
				 this.parent.showFlag = true;
				 this.parent.addOrUpdateFlag = false;
				 this.parent.wupinguihaiCrossAddOrUpdateFlag = false;
				 this.parent.search();
				 this.parent.contentStyleChange();
			       }
			     });
			   } else {
			     this.$message.error(data.msg);
			   }
			 });
		 }
         }
       });
    },
    // 获取uuid
    getUUID () {
      return new Date().getTime();
    },
    // 返回
    back() {
      this.parent.showFlag = true;
      this.parent.addOrUpdateFlag = false;
      this.parent.wupinguihaiCrossAddOrUpdateFlag = false;
      this.parent.contentStyleChange();
    },
    wupintupianUploadChange(fileUrls) {
	    this.ruleForm.wupintupian = fileUrls;
    },
  }
};
</script>
<style lang="scss" scoped>
	.amap-wrapper {
		width: 100%;
		height: 500px;
	}
	
	.search-box {
		position: absolute;
	}
	
	.el-date-editor.el-input {
		width: auto;
	}
	
	.add-update-preview .el-form-item /deep/ .el-form-item__label {
	  	  padding: 0 10px 0 0;
	  	  color: #333;
	  	  width: 140px;
	  	  font-size: 14px;
	  	  line-height: 40px;
	  	  text-align: right;
	  	}
	
	.add-update-preview .el-form-item /deep/ .el-form-item__content {
	  margin-left: 140px;
	}
	
	.add-update-preview .el-input /deep/ .el-input__inner {
	  	  border: 2px solid #797979;
	  	  border-radius: 4px;
	  	  padding: 0 12px;
	  	  outline: none;
	  	  color: #333;
	  	  width: 300px;
	  	  font-size: 14px;
	  	  height: 40px;
	  	}
	
	.add-update-preview .el-select /deep/ .el-input__inner {
	  	  border: 2px solid #797979;
	  	  border-radius: 4px;
	  	  padding: 0 10px;
	  	  outline: none;
	  	  color: #333;
	  	  width: 200px;
	  	  font-size: 14px;
	  	  height: 40px;
	  	}
	
	.add-update-preview .el-date-editor /deep/ .el-input__inner {
	  	  border: 2px solid #797979;
	  	  border-radius: 4px;
	  	  padding: 0 10px 0 30px;
	  	  outline: none;
	  	  color: #333;
	  	  width: 200px;
	  	  font-size: 14px;
	  	  height: 40px;
	  	}
	
	.add-update-preview /deep/ .el-upload--picture-card {
		background: transparent;
		border: 0;
		border-radius: 0;
		width: auto;
		height: auto;
		line-height: initial;
		vertical-align: middle;
	}
	
	.add-update-preview /deep/ .upload .upload-img {
	  	  border: 2px dashed #797979;
	  	  cursor: pointer;
	  	  border-radius: 6px;
	  	  color: #797979;
	  	  width: 150px;
	  	  font-size: 32px;
	  	  line-height: 150px;
	  	  text-align: center;
	  	  height: 150px;
	  	}
	
	.add-update-preview /deep/ .el-upload-list .el-upload-list__item {
	  	  border: 2px dashed #797979;
	  	  cursor: pointer;
	  	  border-radius: 6px;
	  	  color: #797979;
	  	  width: 150px;
	  	  font-size: 32px;
	  	  line-height: 150px;
	  	  text-align: center;
	  	  height: 150px;
	  	}
	
	.add-update-preview /deep/ .el-upload .el-icon-plus {
	  	  border: 2px dashed #797979;
	  	  cursor: pointer;
	  	  border-radius: 6px;
	  	  color: #797979;
	  	  width: 150px;
	  	  font-size: 32px;
	  	  line-height: 150px;
	  	  text-align: center;
	  	  height: 150px;
	  	}
	
	.add-update-preview .el-textarea /deep/ .el-textarea__inner {
	  	  border: 2px solid #797979;
	  	  border-radius: 4px;
	  	  padding: 12px;
	  	  outline: none;
	  	  color: #333;
	  	  width: 400px;
	  	  font-size: 14px;
	  	  height: 120px;
	  	}
</style>
