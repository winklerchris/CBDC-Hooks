/*
 * launchpad.c Hook - NFT based launchpad on the XRPL.
 *
 * Copyright (c) 2022 Chris Winkler.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define HAS_CALLBACK

#include <stdint.h>
#include "hookapi.h"

#define ttNFT_MINT 25
#define ttNFT_CREATE_OFFER 27
#define lsfBURNABLE 0x0001
#define lsfONLY_XRP 0x0002
#define lsfTRANSFERABLE 0x0008
#define tfSELL_OFFER 0x0001
#define MAX_URI_LEN 192
#define URI_LEN 42
#define NUMBER_OF_CATEGORIES 2
#define KEY_SIZE 32
#define NFT_ID_SIZE 32
#define ACCID_SIZE 20
#define ACC_DATA_SIZE 42
#define ACC_DATA_AMOUNT_OFFSET 32
#define ACC_DATA_CATEGORY_OFFSET 40
#define ACC_DATA_RESULT_OFFSET 41

#pragma region Macros
// HASH256 COMMON
#define ENCODE_HASH256_COMMON_SIZE 33U
#define ENCODE_HASH256_COMMON(buf_out, nft_id, field)             \
    {                                                             \
        uint8_t uf = field;                                       \
        buf_out[0] = 0x50U + (uf & 0x0FU);                        \
        *(uint64_t *)(buf_out + 1) = *(uint64_t *)(nft_id + 0);   \
        *(uint64_t *)(buf_out + 9) = *(uint64_t *)(nft_id + 8);   \
        *(uint64_t *)(buf_out + 17) = *(uint64_t *)(nft_id + 16); \
        *(uint64_t *)(buf_out + 25) = *(uint64_t *)(nft_id + 24); \
        buf_out += ENCODE_HASH256_COMMON_SIZE;                    \
    }
#define _05_XX_ENCODE_HASH256_COMMON(buf_out, nft_id, field) \
    ENCODE_HASH256_COMMON(buf_out, nft_id, field);

// NFT ID
#define ENCODE_TOKEN_ID_SIZE 33U
#define ENCODE_TOKEN_ID(buf_out, nft_id) \
    ENCODE_HASH256_COMMON(buf_out, nft_id, 0xAU);
#define _05_10_ENCODE_TOKEN_ID(buf_out, nft_id) \
    ENCODE_TOKEN_ID(buf_out, nft_id);

// NFT SELL OFFER
#define PREPARE_NFT_CREATE_OFFER_SELL_SIZE 293U // 155 + 116 + 22
#define PREPARE_NFT_CREATE_OFFER_SELL(buf_out_master, flags, dest_accid, nft_id, drops_amount)                        \
    {                                                                                                                 \
        uint8_t *buf_out = buf_out_master;                                                                            \
        uint8_t acc[20];                                                                                              \
        uint32_t cls = (uint32_t)ledger_seq();                                                                        \
        hook_account(SBUF(acc));                                                                                      \
        _01_02_ENCODE_TT(buf_out, ttNFT_CREATE_OFFER);     /* uint16  | size   3 */                                   \
        _02_02_ENCODE_FLAGS(buf_out, flags);               /* uint32  | size   5 */                                   \
        _02_04_ENCODE_SEQUENCE(buf_out, 0);                /* uint32  | size   5 */                                   \
        _02_26_ENCODE_FLS(buf_out, cls + 1);               /* uint32  | size   6 */                                   \
        _02_27_ENCODE_LLS(buf_out, cls + 5);               /* uint32  | size   6 */                                   \
        _05_10_ENCODE_TOKEN_ID(buf_out, nft_id);           /* amount  | size  33 */                                   \
        _08_03_ENCODE_ACCOUNT_DST(buf_out, dest_accid);    /* account | size  22 */                                   \
        _06_01_ENCODE_DROPS_AMOUNT(buf_out, drops_amount); /* amount  | size   9 */                                   \
        uint8_t *fee_ptr = buf_out;                                                                                   \
        _06_08_ENCODE_DROPS_FEE(buf_out, 0);                                                 /* amount  | size   9 */ \
        _07_03_ENCODE_SIGNING_PUBKEY_NULL(buf_out);                                          /* pk      | size  35 */ \
        _08_01_ENCODE_ACCOUNT_SRC(buf_out, acc);                                             /* account | size  22 */ \
        int64_t edlen = etxn_details((uint32_t)buf_out, PREPARE_NFT_CREATE_OFFER_SELL_SIZE); /* emitdet | size 1?? */ \
        int64_t fee = etxn_fee_base(buf_out_master, PREPARE_NFT_CREATE_OFFER_SELL_SIZE);                              \
        _06_08_ENCODE_DROPS_FEE(fee_ptr, fee);                                                                        \
    }

#define ENCODE_URI(buf_out, uri, uri_len)                                            \
    {                                                                                \
        buf_out[0] = 0x75U;                                                          \
        buf_out[1] = uri_len > MAX_URI_LEN ? MAX_URI_LEN : uri_len;                  \
        for (int jj = 0; GUARD(MAX_URI_LEN), jj < uri_len && jj < MAX_URI_LEN; ++jj) \
            buf_out[jj + 2] = uri[jj + 0];                                           \
        buf_out += uri_len > MAX_URI_LEN ? MAX_URI_LEN + 2 : uri_len + 2;            \
    }
#define _07_05_ENCODE_URI(buf_out, uri, uri_len) \
    ENCODE_URI(buf_out, uri, uri_len);

#define ENCODE_TRANSFER_FEE_SIZE 3
#define ENCODE_TRANSFER_FEE(buf_out, tf)     \
    {                                        \
        uint16_t utf = tf;                   \
        buf_out[0] = 0x14U;                  \
        buf_out[1] = (utf >> 8) & 0xFFU;     \
        buf_out[2] = (utf >> 0) & 0xFFU;     \
        buf_out += ENCODE_TRANSFER_FEE_SIZE; \
    }
#define _01_04_ENCODE_TRANSFER_FEE(buf_out, transfer_fee) \
    ENCODE_TRANSFER_FEE(buf_out, transfer_fee);

// Calculate NFT ID
#define CALC_NFT_ID_SIZE 32U
#define CALC_NFT_ID(buf_out, flags, fee, hook_accid, taxon, sequence) \
    {                                                                 \
        UINT16_TO_BUF(buf_out, flags);                                \
        UINT16_TO_BUF(buf_out + 2, fee);                              \
        *(uint32_t *)(buf_out + 4) = *(uint32_t *)(hook_accid + 0);   \
        *(uint32_t *)(buf_out + 8) = *(uint32_t *)(hook_accid + 4);   \
        *(uint32_t *)(buf_out + 12) = *(uint32_t *)(hook_accid + 8);  \
        *(uint32_t *)(buf_out + 16) = *(uint32_t *)(hook_accid + 12); \
        *(uint32_t *)(buf_out + 20) = *(uint32_t *)(hook_accid + 16); \
        UINT32_TO_BUF(buf_out + 24, taxon);                           \
        UINT32_TO_BUF(buf_out + 28, sequence);                        \
        buf_out += CALC_NFT_ID_SIZE;                                  \
    }

// TAXON
#define ENCODE_NFTOKEN_TAXON_SIZE 6U
#define ENCODE_NFTOKEN_TAXON(buf_out, taxon) \
    ENCODE_UINT32_UNCOMMON(buf_out, taxon, 0x2A);
#define _02_42_ENCODE_NFTOKEN_TAXON(buf_out, taxon) \
    ENCODE_NFTOKEN_TAXON(buf_out, taxon);

#define PREPARE_MINT_SIMPLE_SIZE 238U
#define PREPARE_MINT_SIMPLE(buf_out_master, flags, taxon, transfer_fee, uri, uri_len)                       \
    {                                                                                                       \
        uint8_t *buf_out = buf_out_master;                                                                  \
        uint8_t acc[20];                                                                                    \
        uint32_t cls = (uint32_t)ledger_seq();                                                              \
        hook_account(SBUF(acc));                                                                            \
        _01_02_ENCODE_TT(buf_out, ttNFT_MINT);             /* uint16  | size   3 */                         \
        _01_04_ENCODE_TRANSFER_FEE(buf_out, transfer_fee); /* uint16  | size   3 */                         \
        _02_02_ENCODE_FLAGS(buf_out, flags);               /* uint32  | size   5 */                         \
        _02_04_ENCODE_SEQUENCE(buf_out, 0);                /* uint32  | size   5 */                         \
        _02_26_ENCODE_FLS(buf_out, cls + 1);               /* uint32  | size   6 */                         \
        _02_27_ENCODE_LLS(buf_out, cls + 5);               /* uint32  | size   6 */                         \
        _02_42_ENCODE_NFTOKEN_TAXON(buf_out, taxon);       /* amount  | size   6 */                         \
        _07_05_ENCODE_URI(buf_out, uri, uri_len);          /* uri     | size   ? */                         \
        uint8_t *fee_ptr = buf_out;                                                                         \
        _06_08_ENCODE_DROPS_FEE(buf_out, 2000);                                    /* amount  | size   9 */ \
        _07_03_ENCODE_SIGNING_PUBKEY_NULL(buf_out);                                /* pk      | size  35 */ \
        _08_01_ENCODE_ACCOUNT_SRC(buf_out, acc);                                   /* account | size  22 */ \
        int64_t edlen = etxn_details((uint32_t)buf_out, PREPARE_MINT_SIMPLE_SIZE); /* emitdet | size 1?? */ \
    }
#pragma endregion

// Begin - Project specific variables
uint64_t close_time = 725842799;
uint8_t nft_uris[NUMBER_OF_CATEGORIES][URI_LEN] = {{"https://dexfi.pro/#/certificates/sec05.jpg"},
                                                   {"https://dexfi.pro/#/certificates/sec10.jpg"}}; // use ipfs in production mode!
uint8_t max_nfts[NUMBER_OF_CATEGORIES] = {10, 5};
uint64_t nft_price[NUMBER_OF_CATEGORIES] = {500000000, 950000000};
uint8_t project_address[] = "rn5JcTebayHdUTw1qV4YsiSzEwJDtVGk19";
// Begin - Project specific variables

uint8_t state_key_nftid[KEY_SIZE];
uint8_t state_data_nftid[NFT_ID_SIZE];
uint8_t state_key_account[KEY_SIZE];
uint8_t state_data_account[ACC_DATA_SIZE];
uint8_t state_key_idx[KEY_SIZE] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9};
uint8_t state_key_paid[KEY_SIZE] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8};
uint8_t state_key_open_refunds[KEY_SIZE] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7};
uint8_t state_data_idx[NUMBER_OF_CATEGORIES * 2];
uint8_t state_data_paid[1];
uint8_t state_data_open_refunds[1];

int64_t cbak(uint32_t reserved)
{
    uint8_t tx_failed = 1;
    TRACESTR("CBAK:");
    // // Originating tx
    int64_t oslot = otxn_slot(0);
    if (oslot < 0)
        rollback(SBUF("Launchpad CB: Could not slot originating txn."), oslot);

    // Meta data otxn
    int64_t mslot = meta_slot(0);
    if (mslot < 0)
        rollback(SBUF("Launchpad CB: Could not slot meta data."), mslot);

    // Tx success
    int64_t tx_res_slot = slot_subfield(mslot, sfTransactionResult, 0);
    if (tx_res_slot < 0)
        rollback(SBUF("Launchpad CB: Could not slot meta.sfTransactionResult"), tx_res_slot);
    uint8_t tx_res_buffer[1];
    slot(SBUF(tx_res_buffer), tx_res_slot);
    tx_failed = tx_res_buffer[0];
    if (tx_failed != 0)
        rollback(SBUF("Launchpad CB: Emitted Tx was not tesSUCCESSful."), INVALID_TXN);

    // Tx type
    int64_t tx_type_slot = slot_subfield(oslot, sfTransactionType, 0);
    if (tx_type_slot < 0)
        rollback(SBUF("Launchpad CB: Could not slot otxn.sfTransactionType"), tx_type_slot);
    uint8_t tx_type_buf[2];
    int64_t bw = slot(SBUF(tx_type_buf), tx_type_slot);
    uint16_t tx_type = UINT16_FROM_BUF(tx_type_buf);

    uint8_t account[ACCID_SIZE];
    int64_t account_slot = slot_subfield(oslot, sfAccount, 0);
    if (account_slot < 0)
        rollback(SBUF("Launchpad CB: Could not slot otxn.sfAccount"), account_slot);
    bw = slot(SBUF(account), account_slot);

    state(SBUF(state_data_idx), SBUF(state_key_idx));

    state(SBUF(state_data_open_refunds), SBUF(state_key_open_refunds));

    switch (tx_type)
    {
    case ttNFT_MINT:
        TRACESTR("CB ttNFT_MINT");
        if (tx_failed != 0)
            rollback(SBUF("Launchpad CB: Could not mint NFT."), tx_failed);
        int64_t taxon_slot = slot_subfield(oslot, sfNFTokenTaxon, 0);
        if (taxon_slot < 0)
            rollback(SBUF("Launchpad CB: Could not slot otxn.sfNFTokenTaxon"), taxon_slot);
        uint8_t taxon_buf[4];
        int64_t bw = slot(SBUF(taxon_buf), taxon_slot);
        uint32_t taxon = UINT32_FROM_BUF(taxon_buf);
        state_key_nftid[(uint8_t)taxon] = ++state_data_idx[(uint8_t)taxon];
        int64_t flag_slot = slot_subfield(oslot, sfFlags, 0);
        if (flag_slot < 0)
            rollback(SBUF("Launchpad CB: Could not slot otxn.sfFlags"), flag_slot);
        uint8_t flag_buf[4];
        bw = slot(SBUF(flag_buf), flag_slot);
        uint32_t flag = UINT32_FROM_BUF(flag_buf);
        int64_t fee_slot = slot_subfield(oslot, sfTransferFee, 0);
        if (fee_slot < 0)
            rollback(SBUF("Launchpad CB: Could not slot otxn.sfTransferFee"), fee_slot);
        uint8_t transfer_fee_buf[2];
        bw = slot(SBUF(transfer_fee_buf), fee_slot);
        uint16_t transfer_fee = UINT16_FROM_BUF(transfer_fee_buf);
        int64_t affected_nodes_slot = slot_subfield(mslot, sfAffectedNodes, 0);
        if (affected_nodes_slot < 0)
            rollback(SBUF("Launchpad CB: Could not slot otxn.sfAffectedNodes"), affected_nodes_slot);
        uint8_t found = 0;
        int64_t minted_nftokens_slot = 0;
        for (int i = 0; GUARD(8), i < 8 && found == 0; ++i)
        {
            int64_t subslot = slot_subarray(affected_nodes_slot, i, 0);
            int64_t final_fields_slot = slot_subfield(subslot, sfFinalFields, 0);
            minted_nftokens_slot = slot_subfield(final_fields_slot, sfMintedNFTokens, 0);
            if (minted_nftokens_slot >= 0)
                found = 1;
        }
        if (found == 0)
            rollback(SBUF("Launchpad CB: Could not slot otxn.sfMintedNFTokens"), minted_nftokens_slot);
        uint8_t minted_nftokens_buf[4];
        bw = slot(SBUF(minted_nftokens_buf), minted_nftokens_slot);
        uint32_t serial = UINT32_FROM_BUF(minted_nftokens_buf) - 1;
        taxon ^= (384160001 * serial + 2459);
        uint8_t *nft_id_buf = state_data_nftid;
        CALC_NFT_ID(nft_id_buf, flag, transfer_fee, account, taxon, serial);
        if (state_set(SBUF(state_data_nftid), SBUF(state_key_nftid)) != NFT_ID_SIZE)
            rollback(SBUF("Launchpad CB: could not write state_data_nftid"), INTERNAL_ERROR);
        if (state_set(SBUF(state_data_idx), SBUF(state_key_idx)) != (uint64_t)NUMBER_OF_CATEGORIES * 2)
            rollback(SBUF("Launchpad CB: could not write state_data_idx"), INTERNAL_ERROR);
        accept(SBUF("Launchpad CB: Stored NFT state."), SUCCESS);
        break;
    case ttNFT_CREATE_OFFER:
        TRACESTR("CB ttNFT_CREATE_OFFER");
        int64_t destination_slot = slot_subfield(oslot, sfDestination, 0);
        if (destination_slot < 0)
            rollback(SBUF("Launchpad CB: Could not slot otxn.sfDestination"), destination_slot);
        bw = slot(SBUF(state_key_account), destination_slot);
        if (state(SBUF(state_data_account), SBUF(state_key_account)) != sizeof(state_data_account))
            rollback(SBUF("Launchpad CB: could not read state_data_account"), INTERNAL_ERROR);
        state_data_account[ACC_DATA_RESULT_OFFSET] = 1;
        if (state_set(SBUF(state_data_account), SBUF(state_key_account)) != ACC_DATA_SIZE)
            rollback(SBUF("Launchpad: could not write state_data_account"), INTERNAL_ERROR);
        ++state_data_open_refunds[0];
        if (state_set(SBUF(state_data_open_refunds), SBUF(state_key_open_refunds)) != sizeof(state_data_open_refunds))
            rollback(SBUF("Launchpad CB: could not write state_key_open_refunds"), INTERNAL_ERROR);
        accept(SBUF("Launchpad CB: Stored ACCOUNT state."), SUCCESS);
        break;
    case ttPAYMENT:
        TRACESTR("CB ttPAYMENT");
        uint8_t project_accid[ACCID_SIZE];
        util_accid(SBUF(project_accid), SBUF(project_address));
        destination_slot = slot_subfield(oslot, sfDestination, 0);
        if (destination_slot < 0)
            rollback(SBUF("Launchpad CB: Could not slot otxn.sfDestination"), destination_slot);
        bw = slot(SBUF(state_key_account), destination_slot);
        uint8_t equal = 0;
        BUFFER_EQUAL(equal, project_accid, state_key_account, ACCID_SIZE);
        if (equal != 1)
        {
            state_set(0, 0, SBUF(state_key_account));
            if (state_data_open_refunds[0] > 0)
                --state_data_open_refunds[0];
            if (state_set(SBUF(state_data_open_refunds), SBUF(state_key_open_refunds)) != sizeof(state_data_open_refunds))
                rollback(SBUF("Launchpad CB: could not write state_key_open_refunds"), INTERNAL_ERROR);
        }
        else
        {
            state_data_paid[0] = 1;
            if (state_set(SBUF(state_data_paid), SBUF(state_key_paid)) != sizeof(state_data_paid))
                rollback(SBUF("Launchpad CB: could not write state_key_paid"), INTERNAL_ERROR);
        }
        accept(SBUF("Launchpad CB: Payment processed."), SUCCESS);
        break;
    default:
        TRACESTR("CB default");
        rollback(SBUF("Launchpad CB: Undefined Tx type."), INVALID_ARGUMENT);
        break;
    }
    return 0;
}

int64_t hook(uint32_t reserved)
{
    enum Action
    {
        setup = 1,
        buy = 2,
        retry = 3,
        refund = 4,
        payout = 5
    };
    enum TxType
    {
        nft_mint = 1,
        nft_offer = 2,
        payment = 3
    };
    typedef struct
    {
        uint8_t tx_type;
        uint8_t *receiver;
        uint8_t *id;
        uint64_t amount;
        char *uri;
        uint8_t taxon;
        uint16_t flags;
    } Tx;
    Tx txs[NUMBER_OF_CATEGORIES];
    uint8_t project_accid[ACCID_SIZE];
    util_accid(SBUF(project_accid), SBUF(project_address));
    uint8_t payout_address[] = "r9BjimZAz1a84k9eHnkRpPbv2aE6p1DThL";
    uint8_t payout_accid[ACCID_SIZE];
    util_accid(SBUF(payout_accid), SBUF(payout_address));
    uint64_t total_amount = 0;
    uint8_t num_of_txs = 0;
    uint8_t category = 255;
    uint16_t nft_transfer_fee = 1000;
    uint16_t nft_mint_flags = lsfONLY_XRP + lsfTRANSFERABLE;
    uint16_t nft_offer_flags = tfSELL_OFFER;
    uint8_t action = 0;

    // Accs
    uint8_t hook_accid[ACCID_SIZE];
    hook_account((uint32_t)hook_accid, ACCID_SIZE);
    if (hook_accid[0] == 0)
        rollback(SBUF("Launchpad: Hook account field missing."), DOESNT_EXIST);
    uint8_t sender_accid[ACCID_SIZE];
    int32_t sender_accid_len = otxn_field(SBUF(sender_accid), sfAccount);
    if (sender_accid_len < ACCID_SIZE)
        rollback(SBUF("Launchpad: sfAccount field missing."), DOESNT_EXIST);

    // Originating tx
    int64_t oslot = otxn_slot(0);
    if (oslot < 0)
        rollback(SBUF("Launchpad: Could not slot originating txn."), NO_FREE_SLOTS);
    int64_t amt_slot = slot_subfield(oslot, sfAmount, 0);
    if (amt_slot < 0)
        rollback(SBUF("Launchpad: Could not slot otxn.sfAmount."), NO_FREE_SLOTS);
    int64_t amt = slot_float(amt_slot);
    if (amt < 0)
        rollback(SBUF("Launchpad: Could not parse amount."), PARSE_ERROR);
    uint64_t amount_in = float_int(amt, 6, 0);
    int64_t is_xrp = slot_type(amt_slot, 1);
    if (is_xrp < 0)
        rollback(SBUF("Launchpad: Could not determine sent amount type."), PARSE_ERROR);
    if (is_xrp != 1)
        rollback(SBUF("Launchpad: IOU not supported."), INVALID_ARGUMENT);
    int64_t dest_tag_slot = slot_subfield(oslot, sfDestinationTag, 0);
    if (dest_tag_slot < 0)
        rollback(SBUF("Launchpad: Could not slot otxn.sfDestinationTag."), NO_FREE_SLOTS);
    uint8_t dest_tag_buf[4];
    dest_tag_slot = slot(SBUF(dest_tag_buf), dest_tag_slot);
    uint32_t destination_tag = UINT32_FROM_BUF(dest_tag_buf);
    if (destination_tag == 0)
        rollback(SBUF("Launchpad: Destination tag must not be 0."), TOO_SMALL);
    action = destination_tag > refund ? payout : destination_tag;
    if (action == buy && (amount_in > nft_price[NUMBER_OF_CATEGORIES - 1] || amount_in < nft_price[0]))
        rollback(SBUF("Launchpad: Invalid amount."), INVALID_ARGUMENT);
    else if (action == buy)
        for (int i = 0; GUARD(NUMBER_OF_CATEGORIES), i < NUMBER_OF_CATEGORIES; ++i)
            if (nft_price[i] == amount_in)
                category = i;

    state(SBUF(state_data_idx), SBUF(state_key_idx));
    uint8_t sold_out = 1;
    for (int i = 0; GUARD(NUMBER_OF_CATEGORIES), i < NUMBER_OF_CATEGORIES; ++i)
        if (state_data_idx[NUMBER_OF_CATEGORIES + i] < max_nfts[i])
            sold_out = 0;
    uint64_t time = (uint64_t)ledger_last_time();
    if (time < 1)
        rollback(SBUF("Launchpad: Could not retrieve last ledger time."), INTERNAL_ERROR);
    uint8_t closed = time > close_time ? 1 : 0;

    switch (action)
    {
    case setup:
        TRACESTR("setup");
        if (sold_out == 1 || closed == 1)
            rollback(SBUF("Launchpad: Launchpad is closed."), INVALID_ARGUMENT);
        if (state_data_idx[0] > 0)
            rollback(SBUF("Launchpad: Launchpad is already set up."), INVALID_ARGUMENT);
        for (uint8_t i = 0; GUARD(NUMBER_OF_CATEGORIES), i < NUMBER_OF_CATEGORIES; ++i)
        {
            if (state_data_idx[i] < max_nfts[i])
            {
                txs[i].tx_type = nft_mint;
                txs[i].flags = nft_mint_flags;
                txs[i].taxon = i;
                txs[i].uri = nft_uris[i];
                ++num_of_txs;
            }
        }
        break;
    case buy:
        TRACESTR("buy");
        if (state_data_idx[0] == 0)
            rollback(SBUF("Launchpad: Launchpad is not set up yet."), INVALID_ARGUMENT);
        if (sold_out == 1 || closed == 1)
            rollback(SBUF("Launchpad: Launchpad is closed."), INVALID_ARGUMENT);
        if (category > NUMBER_OF_CATEGORIES)
            rollback(SBUF("Launchpad: Invalid amount sent."), INVALID_TXN);
        for (int i = 0; GUARD(ACCID_SIZE), i < ACCID_SIZE; ++i)
            state_key_account[i] = sender_accid[i];
        if (state(SBUF(state_data_account), SBUF(state_key_account)) > 0)
            rollback(SBUF("Launchpad: Only one purchase per account."), INVALID_ACCOUNT);
        if (state_data_idx[category] < max_nfts[category])
        {
            txs[0].tx_type = nft_mint;
            txs[0].flags = nft_mint_flags;
            txs[0].taxon = category;
            txs[0].uri = nft_uris[category];
            ++num_of_txs;
        }
        if (++state_data_idx[NUMBER_OF_CATEGORIES + category] > max_nfts[category])
            rollback(SBUF("Launchpad: No tickets available for this category."), category);
        state_key_nftid[category] = state_data_idx[NUMBER_OF_CATEGORIES + category];
        if (state(SBUF(state_data_nftid), SBUF(state_key_nftid)) != sizeof(state_data_nftid) || state_data_nftid[1] != nft_mint_flags)
            rollback(SBUF("Launchpad: Invalid state_data_nftid."), INTERNAL_ERROR);
        txs[num_of_txs].tx_type = nft_offer;
        txs[num_of_txs].flags = nft_offer_flags;
        txs[num_of_txs].receiver = sender_accid;
        txs[num_of_txs].id = state_data_nftid;
        ++num_of_txs;
        for (int i = 0; GUARD(NFT_ID_SIZE), i < NFT_ID_SIZE; ++i)
            state_data_account[i] = state_data_nftid[i];
        uint8_t *state_data_ptr = state_data_account;
        UINT64_TO_BUF(state_data_ptr + ACC_DATA_AMOUNT_OFFSET, amount_in);
        state_data_account[ACC_DATA_CATEGORY_OFFSET] = category;
        if (state_set(SBUF(state_data_account), SBUF(state_key_account)) != ACC_DATA_SIZE)
            rollback(SBUF("Launchpad: could not write state_data_account"), INTERNAL_ERROR);
        if (state_set(SBUF(state_data_idx), SBUF(state_key_idx)) != (uint64_t)NUMBER_OF_CATEGORIES * 2)
            rollback(SBUF("Launchpad: could not write state_data_idx"), INTERNAL_ERROR);
        break;
    case retry:
        TRACESTR("retry");
        for (int i = 0; GUARD(ACCID_SIZE), i < ACCID_SIZE; ++i)
            state_key_account[i] = sender_accid[i];
        if (state(SBUF(state_data_account), SBUF(state_key_account)) != sizeof(state_data_account) || state_data_account[ACC_DATA_RESULT_OFFSET] == 1)
            rollback(SBUF("Launchpad: No open payments."), DOESNT_EXIST);
        for (int i = 0; GUARD(NFT_ID_SIZE), i < NFT_ID_SIZE; ++i)
            state_data_nftid[i] = state_data_account[i];
        if (state_data_idx[category] < max_nfts[category])
        {
            txs[0].tx_type = nft_mint;
            txs[0].flags = nft_mint_flags;
            txs[0].taxon = state_data_account[ACC_DATA_CATEGORY_OFFSET];
            txs[0].uri = nft_uris[state_data_account[ACC_DATA_CATEGORY_OFFSET]];
            ++num_of_txs;
        }
        txs[num_of_txs].tx_type = nft_offer;
        txs[num_of_txs].flags = nft_offer_flags;
        txs[num_of_txs].receiver = sender_accid;
        txs[num_of_txs].id = state_data_nftid;
        ++num_of_txs;
        break;
    case refund:
        TRACESTR("refund");
        if (closed == 0)
            rollback(SBUF("Launchpad: Launchpad is not closed yet."), INVALID_ARGUMENT);
        if (sold_out == 1)
            rollback(SBUF("Launchpad: All NFTs are sold. No refunds."), INVALID_ARGUMENT);
        for (int i = 0; GUARD(ACCID_SIZE), i < ACCID_SIZE; ++i)
            state_key_account[i] = sender_accid[i];
        if (state(SBUF(state_data_account), SBUF(state_key_account)) != sizeof(state_data_account))
            rollback(SBUF("Launchpad: No payments found."), DOESNT_EXIST);
        state_data_ptr = state_data_account;
        txs[0].tx_type = payment;
        txs[0].amount = UINT64_FROM_BUF(state_data_ptr + ACC_DATA_AMOUNT_OFFSET);
        txs[0].receiver = sender_accid;
        ++num_of_txs;
        break;
    case payout:
        TRACESTR("payout");
        if (closed == 0 && sold_out == 0)
            rollback(SBUF("Launchpad: Launchpad is not closed yet."), INVALID_ARGUMENT);
        state(SBUF(state_data_paid), SBUF(state_key_paid));
        if (sold_out == 1 && state_data_paid[0] == 0)
        {
            for (int i = 0; GUARD(NUMBER_OF_CATEGORIES), i < NUMBER_OF_CATEGORIES; ++i)
                total_amount += nft_price[i] * max_nfts[i];
            txs[num_of_txs].tx_type = payment;
            txs[num_of_txs].amount = total_amount;
            txs[num_of_txs].receiver = project_accid;
            ++num_of_txs;
        }
        state(SBUF(state_data_open_refunds), SBUF(state_key_open_refunds));
        if ((sold_out == 1 && state_data_paid[0] == 1) || (sold_out == 0 && state_data_open_refunds[0] == 0))
        {
            txs[num_of_txs].tx_type = payment;
            txs[num_of_txs].amount = destination_tag * 1000000;
            txs[num_of_txs].receiver = payout_accid;
            ++num_of_txs;
        }
        if (num_of_txs == 0)
            rollback(SBUF("Launchpad: No payout possible at the moment."), INVALID_ARGUMENT);
        break;
    default:
        rollback(SBUF("Launchpad: Something went wrong... default."), INVALID_ARGUMENT);
        break;
    }

    //  Submit tx(s)
    etxn_reserve(num_of_txs);
    uint8_t emithash[32];
    int64_t e = 0;
    for (int i = 0; GUARD(NUMBER_OF_CATEGORIES), i < num_of_txs; ++i)
    {
        if (txs[i].tx_type == nft_mint)
        {
            unsigned char mint_tx[PREPARE_MINT_SIMPLE_SIZE + 2 + URI_LEN];
            PREPARE_MINT_SIMPLE(mint_tx, txs[i].flags, txs[i].taxon, nft_transfer_fee, txs[i].uri, URI_LEN);
            e = emit(SBUF(emithash), SBUF(mint_tx));
            if (e < 0)
                rollback(SBUF("Launchpad: Failed to mint NFT!"), e);
        }
        else if (txs[i].tx_type == nft_offer)
        {
            unsigned char offer_tx[PREPARE_NFT_CREATE_OFFER_SELL_SIZE];
            PREPARE_NFT_CREATE_OFFER_SELL(offer_tx, txs[i].flags, txs[i].receiver, txs[i].id, 0);
            e = emit(SBUF(emithash), SBUF(offer_tx));
            if (e < 0)
                rollback(SBUF("Launchpad: Failed to create NFT sell offer!"), e);
        }
        else if (txs[i].tx_type == payment)
        {
            unsigned char tx[PREPARE_PAYMENT_SIMPLE_SIZE];
            PREPARE_PAYMENT_SIMPLE(tx, txs[i].amount, txs[i].receiver, i + 1, 0);
            e = emit(SBUF(emithash), SBUF(tx));
            if (e < 0)
                rollback(SBUF("Loan: Failed to emit XRP!"), e);
        }
    }

    accept(SBUF("Launchpad: Everything worked as expected."), 1);
    return 0;
}
