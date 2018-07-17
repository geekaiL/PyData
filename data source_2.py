# -*- coding: utf-8 -*-
from databaseConnect import DB
from defType import *
from batchRules import get_transport_time, schedule_fetch

no_data_count = 0


def basic_data_query(db1, db2, db3, db4, date):
    date_str1 = date.strftime('%Y-%m-%d')
    date_str2 = date.strftime('%Y%m%d')
    date_str3 = (date - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    ######
    # transport data
    #
    schedule_fetch()
    ######
    # material data
    #
    if len(material_info) == 0:
        print 'querying material_info...'
        # with open('/home/licf/data-dashboard-new/material_info.txt') as f:
        #     tmp = f.readlines()
        #     res = map(lambda x: x.strip('\n').split('\t'), tmp)
        #     for poll_res in res:
        #         district_id = area_map[poll_res[0].decode('utf8')] + 1
        #         if poll_res[1] in material_info:
        #             material_info[poll_res[1]].expiration[district_id] = float(poll_res[2]) * 2 + 1
        #         else:
        #             material_info[poll_res[1]] = Materials(poll_res[1], float(poll_res[2]) * 2 + 1, None, district_id)

        sql = '''
            SELECT
              vc_internal_id,
              (CASE WHEN (IFNULL(l_shelf_life, 0) + IFNULL(l_shelf_life_hour, 0) / 24) = 0
                THEN (CASE WHEN l_turnover_day IS NULL
                  THEN 3
                      ELSE l_turnover_day * 2 + 1 END)
               ELSE IFNULL(l_shelf_life, 0) + IFNULL(l_shelf_life_hour, 0) / 24 END) shelf_life,
              vc_name,
              (CASE WHEN (IFNULL(l_shelf_life, 0) + IFNULL(l_shelf_life_hour, 0) / 24) = 0 
                           AND l_turnover_day IS NULL
                THEN -2333
               ELSE 1 END) if_unmaintained,
               vc_store_condition
            FROM
              dw_ods.bms_ods_bms_material;      
        '''
        db1.cur.execute(sql, )
        res = db1.cur.fetchall()
        print 'query done.'
        for poll_res in res:
            if poll_res[0] in material_info:
                material_info[poll_res[0]].name = poll_res[2]
                material_info[poll_res[0]].expiration[0] = poll_res[1]
            else:
                material_info[poll_res[0]] = Materials(poll_res[0], poll_res[1], poll_res[2], 0, poll_res[4])
            if poll_res[3] == -2233:
                material_info[poll_res[0]].no_data = True
    tmp_count = 0
    ##################
    #  rule_sub
    #
    print 'querying extra rules...'
    for warehouse_index in range(0, len(area_warehouse_code)):
        wms_db = db4[warehouse_index]
        sql = '''
                SELECT
                  b.material_id,
                  b.sssl_b,
                  DATE_FORMAT(a.rkrq, '%%H')
                FROM (SELECT *
                      FROM tprj_rukudan
                      WHERE rkd_type = 1 AND DATE_FORMAT(rkrq, '%%Y-%%m-%%d') = %s) a 
                      LEFT JOIN tprj_rukudan_items b
                    ON a.id = b.rkd_id;
              '''
        wms_db.cur.execute(sql, date_str3)
        res = wms_db.cur.fetchall()
        district_id = area_warehouse_code[warehouse_index]
        for poll_res in res:
            material_id = str(poll_res[0])
            district_material_id = district_id + '_' + material_id
            if district_material_id not in late_material_info:
                late_material_info[district_material_id] = float(poll_res[2])

        sql = '''
                SELECT
                  c.kqbm,
                  b.material_id,
                  IFNULL(b.sjsqsl, 0) - IFNULL(b.sjbhsl, 0)                  
                FROM (SELECT *
                      FROM tprj_buhuodan
                      WHERE DATE_FORMAT(createtime, '%%Y-%%m-%%d') = %s) a
                  LEFT JOIN tprj_buhuodan_items b ON a.id = b.bhd_id
                  LEFT JOIN tprj_kuqu c ON a.kq_id = c.id
                GROUP BY b.material_id, c.kqbm;
              '''
        wms_db.cur.execute(sql, date_str3)
        res = wms_db.cur.fetchall()
        for poll_res in res:
            warehouse_id = district_id + '-' + str(poll_res[0])
            material_id = str(poll_res[1])
            if_reject = int(poll_res[2])
            warehouse_material_id = warehouse_id + '_' + material_id
            if warehouse_material_id not in reject_material_info:
                reject_material_info[warehouse_material_id] = if_reject
    ######
    # rule data
    #
    sql = u"SELECT CONCAT((CASE WHEN region_0 LIKE '%%不补货' THEN '不补货_' ELSE '补货_' END), " \
          u"(CASE WHEN region_1='' THEN region_0 ELSE region_1 END)), wh, mid FROM da_log.da_log_wh_log " \
          u"WHERE dt=%s"
    db3.cur.execute(sql, (date_str3 if date_str3 >= '2018-03-18' else '2018-03-18', ))
    tmp = db3.cur.fetchall()
    for poll_tmp in tmp:
        reason = poll_tmp[0]
        if poll_tmp[1] is not None:
            warehouse_id = str(poll_tmp[1])
        else:
            continue
        if poll_tmp[2] is not None:
            material_id = str(poll_tmp[2])
        else:
            continue
        big_warehouse_id = warehouse_id.split('-')[0]
        if big_warehouse_id == 'MRYXTJ':
            big_warehouse_id = 'MRYXTJW'
            warehouse_id = warehouse_id.split('-')[1]
            warehouse_id = big_warehouse_id + '-' + warehouse_id
        if warehouse_id in qiancang610list:
            big_warehouse_id = 'MRYXSHD'
            warehouse_id = warehouse_id.split('-')[1]
            warehouse_id = big_warehouse_id + '-' + warehouse_id
        spu_id = warehouse_id + '_' + material_id
        # rules_info[spu_id] = replenishment_rule_map[reason] + 1
        # rules_info[spu_id] = dashboard_rule_map[reason] + 1
        rules_info[spu_id] = dashboard_rule_map[reason] + 1

    ######
    # replenishment data
    #
    print 'querying replenishment rule data...'
    sql = "SELECT p.warehouse, d.material, d.quantity " \
          "FROM psm.psm_purchase p, psm.psm_purchase_detail d " \
          "WHERE p.purchase_id=d.purchase_id AND p.batch=%s " \
          "AND p.create_user='psm.sys.batch';"
    db2.cur.execute(sql, (date_str1,))
    tmp = db2.cur.fetchall()
    print 'query done.'
    for poll_tmp in tmp:
        warehouse_id = poll_tmp[0]
        big_warehouse_id = warehouse_id.split('-')[0]
        if big_warehouse_id == 'MRYXTJ':
            big_warehouse_id = 'MRYXTJW'
            warehouse_id = warehouse_id.split('-')[1]
            warehouse_id = big_warehouse_id + '-' + warehouse_id
        if warehouse_id in qiancang610list:
            big_warehouse_id = 'MRYXSHD'
            warehouse_id = warehouse_id.split('-')[1]
            warehouse_id = big_warehouse_id + '-' + warehouse_id
        material_id = poll_tmp[1]
        replenishment_count = poll_tmp[2]
        spu_id = warehouse_id + '_' + material_id
        if spu_id not in replenishment_info:
            replenishment_info[spu_id] = 0
        replenishment_info[spu_id] += replenishment_count
    ######
    # spu data
    #
    print 'querying spu info...'
    spu_sell_info.clear()
    district_rule_info.clear()
    sql = '''
            SELECT 
              vc_warehouse, daqu, vc_material_id, 
              l_kc_sl, huiyuanjia, lilunxiaoliang, 
              new_pinlei, l_xhck_sl, zhanbi, l_xsck_sl, 
              avg_sale_cnt, cost_price, city, new_pinlei
            FROM 
              bi_bak.shj_yuanliao_xiaoshou_infor_all_month
            WHERE
              vc_batch=%s AND yuxiajia=0;      
        '''
    db1.cur.execute(sql, (date_str1,))
    res = db1.cur.fetchall()
    print len(res), 'query done.'
    no_data_sold_count = 0
    no_data_count = 0

    for poll_res in res:
        if poll_res[1] == u'MRYXTJ' or poll_res[1] == u'MRYXTJW' or poll_res[1] == u'MRYXBJ' or poll_res[1] == u'MRYXBJN':
            district_no_city_id = 0
            if poll_res[12] == u'北京' or poll_res[12] == u'北京市':
                district_id = 3
            else:
                district_id = 4
        elif poll_res[1] == u'MRYXSH' or poll_res[1] == u'MRYXSHW' or poll_res[1] == u'MRYXSHD' or poll_res[1] == u'MRYXNJ':
            try:
                district_id = area_map[poll_res[12]]
            except KeyError:
                district_id = 1
            district_no_city_id = 1
        elif poll_res[1] == u'MRYXSZ':
            district_id = 2
            district_no_city_id = 2
        else:
            print 'record no district'
            continue
        warehouse_id = poll_res[0]
        big_warehouse_id = poll_res[0].split('-')[0]
        # if big_warehouse_id == 'MRYXTJW':
        #     big_warehouse_id = 'MRYXTJ'
        #     warehouse_id = poll_res[0].split('-')[1]
        #     warehouse_id = big_warehouse_id + '-' + warehouse_id
        material_id = poll_res[2]
        inventory_volume = poll_res[3]
        unit_price = float(poll_res[4]) if poll_res[4] is not None else 0
        theoretic = poll_res[5]
        category = category_map[poll_res[6]]
        category_id_en = category_list_en[category]
        wastage = poll_res[7]
        sold = poll_res[9]
        sellout = theoretic - sold if poll_res[8] is not None else 0
        avg_sale = poll_res[10] if poll_res[10] is not None else 0
        cost_price = float(poll_res[11]) if poll_res[11] is not None else 0
        if big_warehouse_id == 'MRYXBJN' or big_warehouse_id == 'MRYXSZ':
            is_interior = None
        elif warehouse_id in interior_warehouse_list:
            is_interior = None
        else:
            is_interior = 'WF'
        if material_id in material_info:
            store_condition = material_info[material_id].store_condition
        else:
            store_condition = None
        transport_time = get_transport_time(warehouse_id, is_interior, None, warehouse_id,
                                            material_id, None, store_condition, category_id_en)
        transport_time = 21 - transport_time * 12

        expiration = material_info[material_id].expiration[district_no_city_id + 1]
        if expiration is None:
            expiration = material_info[material_id].expiration[0]
            # if material_info[material_id].no_data:
            #     if material_id not in material_unmaintained:
            #         material_unmaintained[material_id] = [0, material_info[material_id].name]
            #     material_unmaintained[material_id][0] += inventory_volume
            # print material_id, warehouse_id, sold
        expiration_index = expiration_type(expiration)

        spu_id = warehouse_id + '_' + material_id
        if spu_id not in replenishment_info:
            replenishment_count = 0
        else:
            replenishment_count = replenishment_info[spu_id]
        new_spu = SPU(warehouse_id, material_id, district_id, inventory_volume,
                      category, unit_price, theoretic, wastage, expiration_index, expiration,
                      sellout, sold, replenishment_count, avg_sale, cost_price)
        spu_sell_info[new_spu.id] = new_spu
        try:
            new_spu.replenishment_rule = rules_info[new_spu.id]
            if rules_info[new_spu.id] == 2:
                district_material_id = big_warehouse_id + '_' + material_id
                if district_material_id in late_material_info and is_interior == 'WF':
                    if transport_time < late_material_info[district_material_id]:
                        spu_sell_info[new_spu.id].sub_rule = 6
                    else:
                        spu_sell_info[new_spu.id].sub_rule = 4
            if rules_info[new_spu.id] == 1:
                warehouse_material_id = warehouse_id + '_' + material_id
                if warehouse_material_id in reject_material_info:
                    if reject_material_info[warehouse_material_id] > 0:
                        spu_sell_info[new_spu.id].sub_rule = 5
                    else:
                        spu_sell_info[new_spu.id].sub_rule = 3

            district_material_id = str(district_no_city_id + 1) + '_' + material_id
            if district_material_id not in district_rule_info:
                district_rule_info[district_material_id] = []
                for rule_index in range(0, len(dashboard_rule_list)-4):
                    district_rule_info[district_material_id].append(0)
            district_rule_info[district_material_id][rules_info[new_spu.id]] += sold
            district_rule_info[district_material_id][0] += sold

        except KeyError:
            # print new_spu.id, 'has no rules'
            # new_spu.replenishment_rule = 'no rule found'
            new_spu.replenishment_rule = 2
        # sql = "SELECT reason " \
        #       "FROM dw_mryx.gongxu_saleout_buhuo_reason_type " \
        #       "WHERE vc_warehouse=%s AND vc_batch=%s AND vc_material_id=%s;"
        # db.cur.execute(sql, (warehouse_id, date_str1, material_id))
        # tmp = db.cur.fetchall()
        # new_spu.replenishment_rule = tmp[0][0] if len(tmp) != 0 else None
    print no_data_count, no_data_sold_count, 'materials are using the default expiration 1.'
    ######
    # near wastage data
    #
    print 'querying near wastage data...'
    for warehouse_index in range(0, len(area_warehouse_code)):
        wms_db = db4[warehouse_index]
        sql = "SELECT " \
              "  kq.kqbm, " \
              "  ma.internal_id material_id, " \
              "  sum(CASE WHEN datediff(NOW(), kc.scrq) >= ROUND(0.5 * ma.shelf_life) " \
              "             AND datediff(NOW(), kc.scrq) <= ROUND(1 * ma.shelf_life) " \
              "             THEN kc.sl ELSE 0 END) sl, " \
              "  sum(kc.sl) total_sl," \
              "  kq.id kq_id " \
              "FROM " \
              "  tprj_kucun kc " \
              "  JOIN tprj_material ma ON ma.internal_id = kc.material_id " \
              "  JOIN tprj_kuqu kq ON kc.kqid = kq.id " \
              "WHERE " \
              "  ma.shelf_life > 0 " \
              "  AND kq.cklx = 2 " \
              "  AND kc.sl > 0 " \
              "  AND kc.scrq IS NOT NULL " \
              "GROUP BY " \
              "kq.id, ma.internal_id;"
        wms_db.cur.execute(sql)
        res = wms_db.cur.fetchall()
        for poll_res in res:
            warehouse_id = area_warehouse_code[warehouse_index] + '-' + poll_res[0]
            material_id = str(poll_res[1])

            spu_id = warehouse_id + '_' + material_id
            try:
                spu_sell_info[spu_id].near_wastage = float(poll_res[2])
                spu_sell_info[spu_id].real_time_inventory = float(poll_res[3])
            except KeyError:
                # print 'no spu recorded for wastage info', spu_id
                pass

    print 'query done.'
    ########
    # actual_income
    #
    print 'querying income data...'
    sql = '''
        SELECT
          a.sku_payment,
          a.mini_warehouse,
          a.material_internal_id
        FROM
          (
            SELECT
              sum(sku_payment) AS sku_payment,
              mini_warehouse,
              material_internal_id
            FROM dw_mryx.fact_material_sale_info_new
            WHERE pay_day=%s GROUP BY mini_warehouse, material_internal_id 
             ) a JOIN
          (
             SELECT
               vc_material_id,
               vc_warehouse
             FROM
               dw_mryx.goods_saleout_unsale_h
             WHERE
               vc_batch=%s
               AND l_presale_type != 1
               AND (offshelf_flag<=0 OR l_kc_sl_dacang!=0)
             ) b
        ON a.mini_warehouse=b.vc_warehouse AND a.material_internal_id=b.vc_material_id;
        '''
    db1.cur.execute(sql, (int(date_str2), date_str1))
    res = db1.cur.fetchall()
    print 'query done.'
    for poll_pay in res:

        warehouse_id = str(poll_pay[1])
        big_warehouse_id = str(poll_pay[1]).split('-')[0]
        if big_warehouse_id == 'MRYXTJ':
            big_warehouse_id = 'MRYXTJW'
            warehouse_id = str(poll_pay[1]).split('-')[1]
            warehouse_id = big_warehouse_id + '-' + warehouse_id
        if warehouse_id in qiancang610list:
            big_warehouse_id = 'MRYXSHD'
            warehouse_id = warehouse_id.split('-')[1]
            warehouse_id = big_warehouse_id + '-' + warehouse_id
        spu_id = warehouse_id + '_' + str(poll_pay[2])
        if poll_pay[1] is None:
            continue
        if poll_pay[2] is None:
            continue
        try:
            spu_sell_info[spu_id].actual_income += float(poll_pay[0]) if poll_pay[0] is not None else 0
        except KeyError:
            # print spu_id, 'not recorded'
            pass
    ###############
    # profit
    #
    print 'querying profit info...'
    sql = '''
        SELECT
          vc_warehouse,
          material_internal_id,
          final_income_profit
        FROM dw_mryx.da_material_sku_dtl_warehouse
        WHERE pay_day=%s;
    '''
    db1.cur.execute(sql, (int(date_str2), ))
    res = db1.cur.fetchall()
    print 'query done.'

    for poll_res in res:
        warehouse_id = str(poll_res[0])
        big_warehouse_id = str(poll_res[0]).split('-')[0]
        if big_warehouse_id == 'MRYXTJ':
            big_warehouse_id = 'MRYXTJW'
            warehouse_id = str(poll_res[0]).split('-')[1]
            warehouse_id = big_warehouse_id + '-' + warehouse_id
        if warehouse_id in qiancang610list:
            big_warehouse_id = 'MRYXSHD'
            warehouse_id = warehouse_id.split('-')[1]
            warehouse_id = big_warehouse_id + '-' + warehouse_id
        spu_id = warehouse_id + '_' + str(poll_res[1])
        try:
            spu_sell_info[spu_id].profit += float(poll_res[2]) if poll_res[2] is not None else 0
        except KeyError:
            # print spu_id, 'not recorded in profit table.'
            pass
    ##############
    # sum = 0
    # for poll_spu in spu_sell_info:
    #     current_spu = spu_sell_info[poll_spu]
    #     sum += current_spu.sold_count * current_spu.unit_price
    #     warehouse_info[current_spu.warehouse_id].add_spu(current_spu)
    # print 'total sold', sum
    # sum = 0
    # for poll_spu in spu_sell_info:
    #     current_spu = spu_sell_info[poll_spu]
    #     sum += current_spu.theoretic * current_spu.unit_price
    # print 'total sold', sum
    # sum = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    # for poll_spu in spu_sell_info:
    #     if spu_sell_info[poll_spu].expiration == 0:
    #         sum[spu_sell_info[poll_spu].category_id] += 1
    # print sum
    print 'querying cycling info...'
    # sql = "SELECT count(*) FROM bi_bak.shj_zhouzhuan_zhunbei_new WHERE vc_batch=%s;"
    # db1.cur.execute(sql, (date_str1, ))
    # res = db1.cur.fetchall()
    # if res[0] == 0:
    #     sql = "call bi_work.sp_gongxu_lizhi_new_2;"
    #     db1.cur.execute(sql, (date_str1, ))

    sql = "SELECT vc_material_id, daqu, l_kc_sl, avg_sale_cnt, new_pinlei, sku_blg_sl, fenlei " \
          "FROM bi_bak.shj_zhouzhuan_zhunbei_new_his " \
          "WHERE vc_batch=%s"
    db1.cur.execute(sql, (date_str1,))
    res = db1.cur.fetchall()
    print 'query done.'
    for category_index in range(0, len(category_list)):
        cycle_info.append([])
        area_sub_charts = cycle_info[category_index]
        for area_index in range(0, len(area_list)):
            area_sub_charts.append([])
            rule_sub_charts = area_sub_charts[area_index]
            for rule_index in range(0, len(dashboard_rule_list)):
                # for rule_index in range(0, 1):
                #     rule_sub_charts.append([])
                # expiration_sub_charts = rule_sub_charts[rule_index]
                rule_sub_charts.append([[0, 0], [0, 0], [0, 0], [0, 0]])

    district_material_replenishment.clear()
    district_material_inventory.clear()
    district_material_sale.clear()
    for poll_spu in spu_sell_info:
        current_spu = spu_sell_info[poll_spu]
        district_material_id = str(current_spu.district_id) + '_' + current_spu.material_id
        if district_material_id not in district_material_replenishment:
            district_material_replenishment[district_material_id] = 0
        district_material_replenishment[district_material_id] += current_spu.replenishment_count

    for poll_res in res:

        area_index = area_map[poll_res[1]] + 1
        is_district = poll_res[6] == u'大仓'
        # print is_district
        if is_district:
            district_inventory = (poll_res[2] if poll_res[2] is not None else 0) - (
                poll_res[5] if poll_res[5] is not None else 0)
        else:
            district_inventory = 0
        district_avg_sale = poll_res[3] if poll_res[3] is not None else 0
        # district_avg_sale = 0
        # print poll_res[3]
        district_material_id = str(area_map[poll_res[1]] + 1) + '_' + poll_res[0]
        if district_material_id not in district_material_inventory:
            district_material_inventory[district_material_id] = 0
        district_material_inventory[district_material_id] += district_inventory

        if district_material_id not in district_material_sale:
            district_material_sale[district_material_id] = 0
        district_material_sale[district_material_id] += district_avg_sale
        country_material_id = '0_' + poll_res[0]
        if country_material_id not in district_material_sale:
            district_material_sale[country_material_id] = 0
        district_material_sale[country_material_id] += district_avg_sale
        # if district_material_id in district_material_replenishment:
        #     if poll_res[0] == '6004383':
        #         print poll_res[0], poll_res[1], district_inventory, district_avg_sale, district_material_replenishment[district_material_id]
        #     district_avg_sale += district_material_replenishment[district_material_id]
        # print district_material_replenishment[district_material_id]
        try:
            category_index = category_map[poll_res[4]] + 1
        except KeyError:
            # print poll_res[4], 'is not in the categories.'
            continue
        # if poll_res[0] in material_unmaintained:
        #     material_unmaintained[poll_res[0]][0] += district_inventory

        expiration = material_info[poll_res[0]].expiration[area_index]
        if expiration is None:
            expiration = material_info[poll_res[0]].expiration[0]
        expiration_index = expiration_type(expiration) + 1
        if district_avg_sale is not None and district_inventory is not None:
            if district_material_id not in district_rule_info:
                # print district_material_id, 'not recorded in district rule info'
                continue
            else:
                reason_count = []
                total_count = 0
                for poll_reason in range(0, len(dashboard_rule_list)-4):
                    reason_count.append(0)

                for poll_reason in range(0, len(dashboard_rule_list)-4):
                    reason_count[poll_reason] = district_rule_info[district_material_id][poll_reason]
                    total_count += district_rule_info[district_material_id][poll_reason]

            for rule_index in range(0, len(dashboard_rule_list)-4):
                for i in range(0, 2):
                    if i == 0:
                        category_sub_list = cycle_info[0]
                    else:
                        category_sub_list = cycle_info[category_index]
                    for j in range(0, 2):
                        if j == 0:
                            area_sub_list = category_sub_list[0]
                        else:
                            area_sub_list = category_sub_list[area_index]
                        rule_sub_list = area_sub_list[rule_index]
                        for k in range(0, 2):
                            if k == 0:
                                expiration_sub_list = rule_sub_list[0]
                            else:
                                expiration_sub_list = rule_sub_list[expiration_index]

                            expiration_sub_list[0] += district_inventory * reason_count[
                                rule_index] / total_count if total_count != 0 else 0
                            expiration_sub_list[1] += district_avg_sale * reason_count[
                                rule_index] / total_count if total_count != 0 else 0


def add_data(summarize_charts, detail_charts):
    print 'start adding'
    for poll_spu in spu_sell_info:
        current_spu = spu_sell_info[poll_spu]
        area_index = current_spu.district_id + 1
        if isinstance(current_spu.replenishment_rule, str):
            continue  # todo: treat as all rules when no rule found.
        rule_index = current_spu.replenishment_rule
        category_index = current_spu.category_id + 1
        expiration_index = current_spu.expiration + 1
        rule_sub_index = current_spu.sub_rule
        if rule_sub_index is None:
            if rule_index == 1:
                rule_sub_index = 3
            elif rule_index == 2:
                rule_sub_index = 4

        for i in range(0, 2):
            if i == 0:
                # add_list(current_spu, charts[0][expiration_index])
                category_sub_chart = summarize_charts[0]
            else:
                # add_list(current_spu, charts[category_index][expiration_index])
                category_sub_chart = summarize_charts[category_index]
                if detail_charts is not None:
                    category_sub_chart_detail = detail_charts[category_index - 1]
            for j in range(0, 2):
                if j == 0:
                    area_sub_chart = [category_sub_chart[0], ]
                    if detail_charts is not None and i != 0:
                        area_sub_chart_detail = [category_sub_chart_detail[0], ]
                else:
                    if area_index > 3:
                        if area_index < 6:
                            area_sub_chart = [category_sub_chart[1], category_sub_chart[area_index], ]
                        else:
                            area_sub_chart = [category_sub_chart[2], category_sub_chart[area_index], ]
                    else:
                        area_sub_chart = [category_sub_chart[area_index], ]
                    if detail_charts is not None and i != 0:
                        if area_index > 3:
                            if area_index < 6:
                                area_sub_chart_detail = [category_sub_chart_detail[1], category_sub_chart_detail[area_index], ]
                            else:
                                area_sub_chart_detail = [category_sub_chart_detail[2],
                                                         category_sub_chart_detail[area_index], ]
                        else:
                            area_sub_chart_detail = [category_sub_chart_detail[area_index], ]
                for l in range(len(area_sub_chart)):
                    for k in range(0, 2):
                        # for k in range(0, 1):
                        if k == 0:
                            rule_sub_chart = [area_sub_chart[l][0], ]
                            # rule_sub_chart = area_sub_chart[rule_index]
                            if detail_charts is not None and i != 0:
                                rule_sub_chart_detail = [area_sub_chart_detail[l][0], ]
                                # rule_sub_chart_detail = area_sub_chart_detail[rule_index]
                        else:
                            rule_sub_chart = [area_sub_chart[l][rule_index], area_sub_chart[l][rule_sub_index], ]
                            if detail_charts is not None and i != 0:
                                rule_sub_chart_detail = [area_sub_chart_detail[l][rule_index], area_sub_chart_detail[l][rule_sub_index]]

                        for m in range(len(rule_sub_chart)):
                            add_list(current_spu, rule_sub_chart[m][expiration_index])
                            add_list(current_spu, rule_sub_chart[m][0])
                            if detail_charts is not None and i != 0:
                                add_list(current_spu, rule_sub_chart_detail[m], True)
                # add_list(current_spu, category_sub_chart[0][expiration_index])
                # area_sub_chart = category_sub_chart['sub'][area_index]
                # add_list(current_spu, area_sub_chart['total'][expiration_index])
                # rule_sub_chart = area_sub_chart['sub'][rule_index]
                # add_list(current_spu, rule_sub_chart[expiration_index])
    fill_wh_cycle(summarize_charts)

    print 'add done'
    #########################
    # row_count = 1
    # row_count += write_xls(sheets[0], row_count, '全国', '全品类', '全规则', charts[0])
    # category_sub_charts = charts
    # for category_index in range(0, len(category_list)):
    #     row_count += write_xls(sheets[0], row_count, '全国',
    #                            category_list[category_index], '全规则',
    #                            category_sub_charts[category_index]['total'])
    #     area_sub_charts = category_sub_charts[category_index]['sub']
    #     for area_index in range(0, len(area_list)):
    #         row_count += write_xls(sheets[0], row_count, area_list[area_index],
    #                                category_list[category_index], '全规则',
    #                                area_sub_charts[area_index]['total'])
    #         rule_sub_charts = area_sub_charts[area_index]['sub']
    #         for rule_index in range(0, len(replenishment_rule_list)):
    #             row_count += write_xls(sheets[0], row_count, area_list[area_index],
    #                                    category_list[category_index], replenishment_rule_list[rule_index],
    #                                    rule_sub_charts[rule_index])
    #######################


def divide_days(charts, charts_detail, duration):
    for category_index in range(0, len(category_list)):
        area_sub_charts = charts[category_index]
        for area_index in range(0, len(area_list)):
            rule_sub_charts = area_sub_charts[area_index]
            # for rule_index in range(0, len(replenishment_rule_list)):
            for rule_index in range(0, len(dashboard_rule_list)-4):
                # for rule_index in range(0, 1):
                chart = rule_sub_charts[rule_index]
                for expiration_index in range(0, len(chart)):
                    chart[expiration_index] = map(lambda x: x / duration, chart[expiration_index])

    if charts_detail is not None:
        for category_index in range(0, len(category_list) - 1):
            area_sub_charts = charts_detail[category_index]
            for area_index in range(0, len(area_list)):
                rule_sub_charts = area_sub_charts[area_index]
                # for rule_index in range(0, len(replenishment_rule_list)):
                for rule_index in range(0, len(dashboard_rule_list)-4):
                    # for rule_index in range(0, 1):
                    chart = rule_sub_charts[rule_index]
                    for poll_material_id in chart:
                        chart[poll_material_id] = map(lambda x: x / duration, chart[poll_material_id])


def summarize_chart_structure():
    ####
    #  categories -- areas -- rules
    ####
    charts = []
    category_sub_charts = charts
    for category_index in range(0, len(category_list)):
        category_sub_charts.append([])
        area_sub_charts = category_sub_charts[category_index]
        for area_index in range(0, len(area_list)):
            area_sub_charts.append([])
            rule_sub_charts = area_sub_charts[area_index]
            # for rule_index in range(0, len(replenishment_rule_list)):
            for rule_index in range(0, len(dashboard_rule_list)):
                # for rule_index in range(0, 1):
                tmp_list = []
                for expiration_index in range(0, len(expiration_list_cn)):
                    tmp_list.append([0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                     0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                     0, 0])
                rule_sub_charts.append(tmp_list)
                # rule_sub_charts.append(
                #     [
                #         [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                #         [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                #         [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                #     ])
    return charts


def detail_chart_structure():
    ####
    #  categories -- areas -- rules -- materials dict
    ####
    charts = []
    for category_index in range(0, len(category_list)):
        charts.append([])
        area_sub_charts = charts[category_index]
        for area_index in range(0, len(area_list)):
            area_sub_charts.append([])
            rule_sub_charts = area_sub_charts[area_index]
            # for rule_index in range(0, len(replenishment_rule_list)):
            for rule_index in range(0, len(dashboard_rule_list)):
                # for rule_index in range(0, 1):
                rule_sub_charts.append({})
    return charts


def add_list(spu, chart, is_detail=False):
    if is_detail:
        try:
            chart = chart[spu.material_id]
        except KeyError:
            district_material_id = str(spu.district_id + 1) + '_' + spu.material_id
            chart[spu.material_id] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                      0, 0, 0, 0, 0, material_info[spu.material_id].name,
                                      expiration_list_cn[spu.expiration + 1], 0, 0, 0,
                                      0, 0, float(spu.expiration_value)]
            chart = chart[spu.material_id]
            if district_material_id in district_material_inventory:
                chart[12] += float(district_material_inventory[district_material_id])
                chart[14] += float(district_material_sale[district_material_id])
            else:
                # print 'district inventory is not recorded', district_material_id
                pass

    # chart[0] += spu.actual_income
    chart[0] += float(spu.sold_count * spu.unit_price)
    chart[1] += float(spu.sellout_count * spu.unit_price)
    chart[2] += float(spu.sellout_count)
    chart[3] += float(spu.wastage * spu.cost_price)
    chart[4] += float(spu.theoretic)
    chart[5] += float(spu.inventory * spu.unit_price)
    chart[6] += float(spu.theoretic * spu.unit_price)
    chart[7] += float(spu.sold_count)
    chart[8] += float(spu.replenishment_count * spu.unit_price)
    chart[9] += float(spu.avg_sale)
    chart[10] += float(spu.sold_count * spu.cost_price)
    chart[11] += float(spu.actual_income)
    chart[13] += float(spu.inventory)
    chart[17] += float(spu.near_wastage) if spu.near_wastage is not None else 0
    chart[18] += float(spu.near_wastage * spu.unit_price) if spu.near_wastage is not None else 0
    chart[19] += float(spu.real_time_inventory) if spu.real_time_inventory is not None else 0
    chart[20] += float(spu.real_time_inventory * spu.unit_price) if spu.real_time_inventory is not None else 0
    chart[21] += float(spu.profit) if spu.profit is not None else 0


def fill_wh_cycle(chart):
    for category_index in range(0, len(category_list)):
        # for area_index in range(0, len(area_list)-2):
        for area_index in range(0, 4):
            # for rule_index in range(0, len(replenishment_rule_list)):
            for rule_index in range(0, len(dashboard_rule_list)-2):
                if rule_index == 3:
                    rule_index_cycle = 1
                elif rule_index == 4:
                    rule_index_cycle = 2
                else:
                    rule_index_cycle = rule_index
                for expiration_index in range(0, len(expiration_list_cn)):
                    tmp = cycle_info[category_index][area_index][rule_index_cycle][expiration_index]
                    # sold_count = float(chart[category_index][area_index][rule_index][expiration_index][10])
                    chart[category_index][area_index][rule_index][expiration_index][12] = float(tmp[0])
                    chart[category_index][area_index][rule_index][expiration_index][14] = float(tmp[1])


def read_yesterday_old(sheet, charts):
    row_num = sheet.nrows
    for poll_row in range(1, row_num):
        row_data = sheet.row_values(poll_row)
        # rule_index = replenishment_rule_map[row_data[3]] + 1
        expiration_index = expiration_map[row_data[0]]
        # if rule_index != 0:
        #     continue
        category_index = category_map[row_data[2]] + 1
        area_index = area_map[row_data[1]] + 1
        rule_index = history_rule_map[row_data[3]]
        sub_chart = charts[category_index][area_index][rule_index][expiration_index]
        sub_chart[0] = float(row_data[7])
        sub_chart[1] = float(row_data[14])


def save_charts(charts_single, charts_single_detail, date_str):
    if charts_single is not None:
        file_name_charts_single = charts_dir + date_str + '_' + 'charts.dat'
        with open(file_name_charts_single, 'w') as f:
            json.dump({'data': charts_single}, f)
        os.chmod(file_name_charts_single, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    if charts_single_detail is not None:
        file_name_charts_detail = charts_dir + date_str + '_' + 'charts_detail.dat'
        with open(file_name_charts_detail, 'w') as f:
            json.dump({'data': charts_single_detail}, f)
        os.chmod(file_name_charts_detail, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)


def generate_charts(date, if_delete=False):
    print 'start fetching data ', date.strftime('%Y-%m-%d')
    flag_need_fetch = True
    db = DB('dw')
    db1 = DB('psm')
    db2 = DB('hive')
    db_wms = [DB('bj-wms'), DB('tj-wms'), DB('sh-wms'), DB('sz-wms')]

    charts_single = summarize_chart_structure()
    charts_single_detail = detail_chart_structure()
    charts_yesterday = summarize_chart_structure()
    yesterday_date = date - datetime.timedelta(days=1)
    yesterday_file_name = charts_dir + yesterday_date.strftime('%Y-%m-%d') + '_' + 'charts.dat'
    file_name = charts_dir + date.strftime('%Y-%m-%d') + '_' + 'charts.dat'
    # file_name_detail = charts_dir + date.strftime('%Y-%m-%d') + '_' + 'charts_detail.dat'
    if os.path.exists(file_name):
        flag_need_fetch = False
        if if_delete:
            flag_need_fetch = True
            os.remove(file_name)

    if flag_need_fetch:
        if not os.path.exists(yesterday_file_name):
            print 'need to fetch yesterday data ', yesterday_date.strftime('%Y-%m-%d')
            basic_data_query(db, db1, db2, db_wms, yesterday_date)
            add_data(charts_yesterday, None)
            save_charts(charts_yesterday, None, yesterday_date.strftime('%Y-%m-%d'))
        basic_data_query(db, db1, db2, db_wms, date)
        add_data(charts_single, charts_single_detail)
        save_charts(charts_single, charts_single_detail, date.strftime('%Y-%m-%d'))
    db.disconnect()
    db1.disconnect()
    db2.disconnect()
    for poll_db in db_wms:
        poll_db.disconnect()


if __name__ == "__main__":
    date = datetime.datetime.now() + datetime.timedelta(days=-day_before)
    generate_charts(date, True)
