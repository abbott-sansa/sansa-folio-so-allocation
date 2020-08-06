/**
 * Sales Order Allocation
 * Customises the UI to streamline the management of SO Allocation Rules.
 *
 * Version      Date                Author                      Remarks
 * 1.0          04 Mar 2020         Chris Abbott                N/A
 *
 * @NApiVersion 2.x
 * @NScriptType UserEventScript
 * @NModuleScope SameAccount
 */
define(['N/search', 'N/ui/serverWidget'],

function(search, serverWidget) {
   
    /**
     * Function definition to be triggered before record is loaded.
     *
     * @param {Object} scriptContext
     * @param {Record} scriptContext.newRecord - New record
     * @param {string} scriptContext.type - Trigger type
     * @param {Form} scriptContext.form - Current form
     * @Since 2015.2
     */
    function beforeLoad(scriptContext) {
        // Add the dynamic fields for each Rule Group.
        search.create({
            type: 'customrecord_sansa_so_all_rule_group',
            columns: ['internalid', 'name', 'custrecord_sansa_so_all_rule_group_fld_l'],
            filters: ['custrecord_sansa_so_all_rule_group_fld', search.Operator.NONEOF, '@NONE@']
        }).run().each(function (result) {
            var field_config = {
                id: 'custpage_sansa_select_' + result.getValue({name: 'internalid'}),
                type: serverWidget.FieldType.TEXT,
                label: 'Rule Value (' + result.getValue({name: 'name'}) + ')'
            };

            if (result.getValue({name: 'custrecord_sansa_so_all_rule_group_fld_l'})) {
                field_config.type = serverWidget.FieldType.MULTISELECT;
                field_config.source = result.getValue({name: 'custrecord_sansa_so_all_rule_group_fld_l'});
            }

            var dynamic_field = scriptContext.form.addField(field_config);
            scriptContext.form.insertField({field: dynamic_field, nextfield: 'custrecord_sansa_so_all_rule_campaign'});

            return true;
        });
    }

    return {
        beforeLoad: beforeLoad
    };
    
});
